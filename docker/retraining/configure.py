"""Save concrete operator inputs after reviewed source-only images exist.

Reads metadata/file existence only; passwords and scoped credential files are
referenced by path and never read, copied or printed. No runtime operation.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('repository','evidence','orchestration-state','core-state','baseline-bundle','baseline-reference'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--release-state',type=Path,default=Path.home()/'.local/state/ats-mlops')
    parser.add_argument('--training-image',required=True);parser.add_argument('--monitoring-image',required=True)
    parser.add_argument('--packager-image',default='sha256:574c48deb107cba3f0bdc982d3aa0ab1500de58ab1694052bb92efbcab13ec91')
    parser.add_argument('--airflow-url',default='http://127.0.0.1:8080');parser.add_argument('--port',type=int,default=3018)
    args=parser.parse_args();repo=args.repository.resolve();output=args.evidence.resolve()
    for image in (args.training_image,args.monitoring_image,args.packager_image):
        if not re.fullmatch('sha256:[0-9a-f]{64}',image):raise ValueError('exact_image_id_required')
    if output.exists():raise ValueError('new_evidence_directory_required')
    if subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],cwd=repo).strip():raise ValueError('clean_source_required')
    source=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
    read=lambda path:json.loads(path.read_bytes())
    state=read(args.release_state/'state.json');active=state['active']
    manifest=read(args.release_state/active['package_path']/'release_manifest.json')
    password=args.orchestration_state.resolve()/'passwords.json';credentials=args.core_state.resolve()/'dvc.env'
    if not password.is_file() or not credentials.is_file():raise ValueError('existing_private_files_required')
    output.mkdir(mode=0o700,parents=True)
    def save(name,value):
        path=output/name;path.write_text(json.dumps(value,sort_keys=True)+'\n');path.chmod(0o600)
    save('serving-identity.json',manifest['serving'])
    batch={'source_repository':str(repo),'source_git_sha':source,'airflow_container':'ats-core-airflow-1',
        'training_image_id':args.training_image,'airflow_url':args.airflow_url,'password_file':str(password),
        'release_state':str(args.release_state.resolve()),'artifacts':str(output/'host-artifacts'),
        'serving_identity':str(output/'serving-identity.json'),'packager_image':args.packager_image,
        'registry_transport':{'network':'ats-core_default','tracking_uri':'http://mlflow:5000'}}
    save('batch.json',batch)
    save('exercise.json',{'batch_config':str(output/'batch.json'),'output':str(output/'actual'),
        'baseline_bundle':str(args.baseline_bundle.resolve()),'baseline_reference':str(args.baseline_reference.resolve()),
        'monitoring_config':str(repo/'configs/monitoring.yaml'),'monitoring_image':args.monitoring_image,
        'dvc_env_file':str(credentials),'project':'ats-t17-'+source[:10],'port':args.port,'v2_namespace':'ats-t17-v2-20261027'})
    print(json.dumps({'outcome':'prepared_only','source_git_sha':source,'exercise_config':str(output/'exercise.json'),'runtime_effects':0}))


if __name__=='__main__':main()
