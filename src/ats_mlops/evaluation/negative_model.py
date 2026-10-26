"""Explicit controlled negative-artifact fixture; never a training alternative.

The parent is a checked qualified model. No estimator is fitted. The only
perturbation reverses the fitted logistic decision function, declared before any
new score is computed. Log a fresh real MLflow fault run/model identity and keep
an immutable derivation receipt; never reattribute the fault to the parent run.
"""
from copy import deepcopy
import argparse
import json
from pathlib import Path
import shutil


def invert_coefficients(model):
    classifier=model.named_steps["classifier"]
    if classifier.classes_.tolist()!=[0,1] or classifier.coef_.shape[0]!=1 or classifier.intercept_.shape!=(1,):
        raise ValueError("negative_fixture_binary_classifier_required")
    classifier.coef_=-classifier.coef_
    classifier.intercept_=-classifier.intercept_
    return model


def create(*,source_root,parent,manifest,admission,output,image):
    # Deliberately lazy: ordinary CLI/unit imports need no ML dependency.
    from ats_mlops.data.validate import require_valid
    from ats_mlops.data.dataset import load_manifest,canonical_json,sha256
    from ats_mlops.lineage.manifest import source_environment,file_hash,tree_inventory,inventory_hash,write_json
    from ats_mlops.model.training import portable_code
    from ats_mlops.model.scoring import pair_frame,score_rows
    from ats_mlops.evaluation.ranking import evaluate_ranking
    from ats_mlops.quality.artifacts import strict_inventory
    import mlflow
    from mlflow.models import infer_signature
    import numpy as np

    parent,source_root,output=Path(parent),Path(source_root),Path(output)
    if output.exists():raise ValueError("new_controlled_fault_output_required")
    output.mkdir(parents=True)
    record=json.loads((parent/"run_manifest.json").read_bytes())
    pipeline=json.loads((parent/"pipeline_manifest.json").read_bytes())
    artifact=parent/"model"
    if strict_inventory(artifact)!=record["model"]["files"]:raise ValueError("negative_parent_inventory_changed")
    rows=require_valid(Path(manifest),output/"guard",admission_report=Path(admission))
    current=source_environment(source_root,image,runtime_lock=Path('/opt/requirements-ml.lock'))
    if any(current[key]!=record[key] for key in current):raise ValueError("negative_parent_source_environment_mismatch")
    metadata=load_manifest(Path(manifest))
    if file_hash(Path(manifest))!=record["data_manifest_sha256"] or file_hash(Path(admission))!=record["reports_sha256"]["admission"]:
        raise ValueError("negative_parent_dataset_mismatch")
    derivation={"scope":"controlled_negative_artifact_fault","operation":"negate_fitted_logistic_coefficients_and_intercept",
        "source_git_sha":current["git_sha"],"parent_model_sha256":record["model"]["sha256"],
        "parent_run_manifest_sha256":file_hash(parent/"run_manifest.json"),"parent_training_mlflow":record["mlflow"],
        "fit_events":0,"selected_for_E5":False}
    write_json(output/"fault-intent.json",derivation)  # BEFORE mutation/scoring/remote logging
    model=mlflow.sklearn.load_model(str(artifact))
    model=invert_coefficients(model)
    if model.named_steps["classifier"].get_params()!=record["effective_classifier_params"]:
        raise ValueError("negative_fixture_changed_hyperparameters")
    scores=score_rows(model,rows["validation"])
    metrics=evaluate_ranking(rows["validation"],scores,group_ids=metadata["splits"]["validation"]["vacancy_ids"])
    metrics.pop("elapsed_seconds")
    if metrics["status"]!="defined":raise ValueError("negative_fixture_undefined_validation")
    write_json(output/"metrics.json",metrics)
    write_json(output/"scores.json",{"rows":[{"vacancy_id":key[0],"candidate_id":key[1],"score":float(value)} for key,value in sorted(scores.items())],
        "parameters":{"score_decimals":12}})
    transformer=model.named_steps["pair_features"]
    write_json(output/"features.json",{"vocabulary":{token:int(index) for token,index in transformer.vectorizer_.vocabulary_.items()},
        "idf":transformer.vectorizer_.idf_.tolist(),"transformed_sha256":{role:sha256(np.asarray(transformer.transform(pair_frame(rows[role])),dtype='<f8').tobytes()) for role in ('train','validation')}})
    provenance={key:deepcopy(value) for key,value in record.items() if key not in {"mlflow","model","reports_sha256"}}
    provenance["artifact_derivation"]=derivation
    write_json(output/"input-provenance.json",provenance)
    mlflow.set_experiment("ats-controlled-artifact-faults")
    with mlflow.start_run(tags={"artifact_origin":"controlled_negative_artifact_fault","training_git_sha":current["git_sha"],
        "parent_training_run_id":record["mlflow"]["run_id"],"fit_events":"0","selected_for_E5":"false","evidence_scope":"demo_only"}) as run:
        example=pair_frame(rows["validation"][:2])
        with portable_code(source_root) as code:
            info=mlflow.sklearn.log_model(model,name="negative-artifact",code_paths=[str(code)],serialization_format="cloudpickle",
                pyfunc_predict_fn="predict_proba",pip_requirements=[f"{name}=={value}" for name,value in provenance["environment"]["packages"].items()],
                signature=infer_signature(example,model.predict_proba(example)),input_example=example,
                metadata={"feature_version":record["features"]["version"],"classes":[0,1],"training_git_sha":current["git_sha"],
                    "features_sha256":record["features_sha256"],"artifact_origin":"controlled_negative_artifact_fault"},extra_files=[str(output/"input-provenance.json")])
        registered=mlflow.register_model(info.model_uri,record["mlflow"]["model_name"])
        exact=f"models:/{registered.name}/{registered.version}"
        version=mlflow.MlflowClient().get_model_version(registered.name,registered.version)
        if version.run_id!=run.info.run_id or version.run_id==record["mlflow"]["run_id"]:raise ValueError("negative_fixture_identity_not_fresh")
        download=output/"download";download.mkdir()
        local=Path(mlflow.artifacts.download_artifacts(artifact_uri=exact,dst_path=str(download)))
        shutil.copytree(local,output/"model");shutil.rmtree(download)
        files=tree_inventory(output/"model")
        identity={"model_name":registered.name,"model_version":str(registered.version),"run_id":run.info.run_id,
                  "experiment_id":run.info.experiment_id,"exact_uri":exact,"source":version.source}
        result={**provenance,"mlflow":identity,"model":{"files":files,"sha256":inventory_hash(files),"payload_sha256":files["model.pkl"]},
            "reports_sha256":{"admission":file_hash(Path(admission)),"training":file_hash(output/"guard/data-training.json"),
                              "validation":file_hash(output/"metrics.json"),"derivation":file_hash(output/"fault-intent.json")}}
        write_json(output/"run_manifest.json",result)
        derived=deepcopy(pipeline);derived["artifact_derivation"]=derivation
        derived["execution_kind"]="controlled_fault_derivation; guarded input/config inherited; no new pipeline fit"
        derived["training"].update(run_manifest_sha256=file_hash(output/"run_manifest.json"),model_sha256=result["model"]["sha256"],mlflow=identity)
        derived["evaluation"]={name:file_hash(output/name) for name in ('metrics.json','scores.json','features.json')}
        write_json(output/"pipeline_manifest.json",derived)
        receipt={**derivation,"candidate_mlflow":identity,"candidate_model_sha256":result["model"]["sha256"],
            "candidate_run_manifest_sha256":file_hash(output/"run_manifest.json"),"candidate_pipeline_manifest_sha256":file_hash(output/"pipeline_manifest.json")}
        write_json(output/"fault-provenance.json",receipt)
        for name in ('fault-intent.json','fault-provenance.json','input-provenance.json','run_manifest.json','pipeline_manifest.json','metrics.json','scores.json','features.json'):
            mlflow.log_artifact(str(output/name),artifact_path='controlled-fault')
        mlflow.set_tag('run_manifest_sha256',file_hash(output/'run_manifest.json'))
    return receipt


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('source-root','parent','manifest','admission','output','image'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args(argv)
    try:
        result=create(source_root=args.source_root,parent=args.parent,manifest=args.manifest,admission=args.admission,
                      output=args.output,image=json.loads(args.image.read_bytes()))
        print(json.dumps({'outcome':'created_controlled_fault','candidate_mlflow':result['candidate_mlflow'],
            'model_sha256':result['candidate_model_sha256'],'fit_events':0}));return 0
    except Exception:
        print('{"outcome":"failed","reason":"controlled_negative_artifact_failed"}');return 1

if __name__=='__main__':raise SystemExit(main())
