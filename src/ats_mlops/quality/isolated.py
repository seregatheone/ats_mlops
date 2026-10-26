"""Artifact-only, offline scoring in a fresh pinned container for each model."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

from ats_mlops.data.dataset import canonical_json, sha256
from ats_mlops.quality.policy import GateError, require


BOOTSTRAP = r'''
import hashlib, importlib.metadata, importlib.util, json, os, pathlib, sys
def canonical(value):
    return (json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def check(condition):
    if not condition:raise ValueError('isolated_contract_failed')
try:
    check(sys.flags.isolated and sys.dont_write_bytecode)
    check(importlib.util.find_spec('ats_mlops') is None)
    check(not any(name=='ats_mlops' or name.startswith('ats_mlops.') for name in sys.modules))
    check(not any(os.environ.get(name) for name in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN','MLFLOW_TRACKING_URI')))
    request=json.loads(pathlib.Path('/input/request.json').read_text())
    record=request['record'];artifact=pathlib.Path('/artifact')
    files={}
    for path in sorted(artifact.rglob('*')):
        check(not path.is_symlink())
        check('__pycache__' not in path.relative_to(artifact).parts and path.suffix.lower() not in ('.pyc','.pyo'))
        if path.is_file():files[path.relative_to(artifact).as_posix()]=digest(path)
        else:check(path.is_dir())
    check(files==record['model']['files'])
    check(hashlib.sha256(canonical(files)).hexdigest()==record['model']['sha256'])
    check(files['model.pkl']==record['model']['payload_sha256'])
    source={name[len('code/ats_mlops/'):]:value for name,value in files.items() if name.startswith('code/ats_mlops/')}
    check(source==record['source_sha256'])
    check(hashlib.sha256(canonical(source)).hexdigest()==record['source_tree_sha256'])
    check(sys.version.split()[0]==record['environment']['python'])
    for name,value in record['environment']['packages'].items():check(importlib.metadata.version(name)==value)
    check(digest(pathlib.Path('/opt/requirements-ml.lock'))==record['environment']['lockfile_sha256'])
    import yaml
    metadata=yaml.safe_load((artifact/'MLmodel').read_text())
    native=metadata['flavors']['sklearn'];pyfunc=metadata['flavors']['python_function']
    check(native['code']=='code' and native['pickled_model']=='model.pkl' and native['serialization_format']=='cloudpickle')
    check(pyfunc['code']=='code' and pyfunc['model_path']=='model.pkl' and pyfunc['loader_module']=='mlflow.sklearn' and pyfunc['predict_fn']=='predict_proba')
    check(metadata['run_id']==record['mlflow']['run_id'])
    check(metadata['metadata']=={'classes':[0,1],'feature_version':record['features']['version'],
          'features_sha256':record['features_sha256'],'training_git_sha':record['git_sha']})
    check(record['mlflow']['source']=='models:/'+metadata['model_id'])
    registered=yaml.safe_load((artifact/'registered_model_meta').read_text())
    check(registered['model_name']==record['mlflow']['model_name'] and str(registered['model_version'])==record['mlflow']['model_version'])
    import mlflow.sklearn
    check(importlib.util.find_spec('ats_mlops') is None)
    check(not any(name=='ats_mlops' or name.startswith('ats_mlops.') for name in sys.modules))
    model=mlflow.sklearn.load_model(str(artifact))
    from ats_mlops.model.scoring import score_rows
    import ats_mlops.features.pairs as features
    check(features.FEATURE_VERSION==record['features']['version'])
    transformer=model.named_steps['pair_features']
    check(transformer.feature_version_==record['features']['version'])
    check(list(transformer.get_feature_names_out())==record['features']['names'])
    check(transformer.max_features==record['params']['features']['max_features'])
    check(list(transformer.ngram_range)==record['params']['features']['ngram_range'])
    check(transformer.vectorizer_.max_features==transformer.max_features)
    check(transformer.vectorizer_.ngram_range==transformer.ngram_range)
    from sklearn.linear_model import LogisticRegression
    classifier=model.named_steps['classifier']
    check(isinstance(classifier,LogisticRegression))
    check(classifier.get_params()==record['effective_classifier_params'])
    check(all(classifier.get_params()[name]==value for name,value in record['params']['classifier'].items()))
    check(classifier.random_state==record['seed'])
    for name,module in list(sys.modules.items()):
        if name=='ats_mlops' or name.startswith('ats_mlops.'):
            check(pathlib.Path(module.__file__).resolve().is_relative_to(artifact/'code'))
    scores=score_rows(model,request['rows'])
    output=[{'vacancy_id':key[0],'candidate_id':key[1],'score':value} for key,value in sorted(scores.items())]
    print(json.dumps({'outcome':'passed','scores':output,'receipt':{
       'isolated':True,'bytecode_disabled':True,'prior_project_import':False,'network':False,
       'training_checkout_mounted':False,'credentials':False,'model_sha256':record['model']['sha256'],
       'source_tree_sha256':record['source_tree_sha256'],'rows':len(output),
       'python':sys.version.split()[0],'feature_version':features.FEATURE_VERSION}},allow_nan=False))
except Exception:
    print(json.dumps({'outcome':'rejected','reason':'isolated_model_scoring_failed'}))
    raise SystemExit(1)
'''


class DockerScorer:
    """Host coordinator; no Docker socket, source or credentials enter scorers."""

    def __call__(self, model, rows):
        record = model["record"]
        image = record["environment"]["image"]["id"]
        try:
            inspected = subprocess.run(["docker", "image", "inspect", image], check=True,
                                       capture_output=True, text=True, timeout=15)
            require(json.loads(inspected.stdout)[0]["Id"] == image, "runtime_image_identity_mismatch")
        except (subprocess.SubprocessError, OSError, ValueError, KeyError, IndexError):
            raise GateError("pinned_runtime_image_unavailable") from None
        name = "ats-quality-" + uuid.uuid4().hex[:16]
        with tempfile.TemporaryDirectory(prefix="ats-quality-input-") as directory:
            payload = {"record": record, "rows": [{key: row[key] for key in
                       ("vacancy_id", "candidate_id", "vacancy_text", "candidate_text")} for row in rows]}
            Path(directory, "request.json").write_bytes(canonical_json(payload))
            args = ["docker", "run", "--rm", "--pull=never", "--name", name,
                    "--user", f"{os.getuid()}:{os.getgid()}",
                    "--network", "none", "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,size=128m",
                    "--cpus", "1", "--memory", "1536m", "--workdir", "/tmp",
                    "--mount", f"type=bind,source={model['artifact'].resolve()},target=/artifact,readonly",
                    "--mount", f"type=bind,source={directory},target=/input,readonly",
                    "-e", "HOME=/tmp", "-e", "USER=ats-offline", "-e", "LOGNAME=ats-offline",
                    "-e", "PYTHONPATH=", "-e", "MLFLOW_TRACKING_URI=", "-e", "AWS_ACCESS_KEY_ID=",
                    "-e", "AWS_SECRET_ACCESS_KEY=", "-e", "AWS_SESSION_TOKEN=",
                    "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
                    "--entrypoint", "/opt/ml-venv/bin/python", image, "-I", "-B", "-c", BOOTSTRAP]
            try:
                completed = subprocess.run(args, capture_output=True, text=True, timeout=120)
            except subprocess.TimeoutExpired:
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=20)
                raise GateError("isolated_model_scoring_timeout") from None
            except OSError:
                raise GateError("isolated_model_scoring_failed") from None
            try:
                output = json.loads(completed.stdout.strip().splitlines()[-1])
                require(completed.returncode == 0 and output["outcome"] == "passed", "isolated_model_scoring_failed")
                receipt = output["receipt"]
                require(receipt["model_sha256"] == record["model"]["sha256"]
                        and receipt["source_tree_sha256"] == record["source_tree_sha256"]
                        and receipt["isolated"] is True and receipt["prior_project_import"] is False,
                        "isolated_scoring_identity_mismatch")
                scores = {}
                for item in output["scores"]:
                    require(set(item) == {"vacancy_id", "candidate_id", "score"}, "invalid_scoring_response")
                    key = (item["vacancy_id"], item["candidate_id"])
                    require(key not in scores, "duplicate_scoring_response")
                    scores[key] = item["score"]
                receipt.update({"runtime_image_id": image, "bootstrap_sha256": sha256(BOOTSTRAP.encode()),
                                "scores_sha256": sha256(canonical_json(output["scores"]))})
                return scores, receipt
            except (ValueError, KeyError, TypeError, IndexError):
                raise GateError("isolated_model_scoring_failed") from None
