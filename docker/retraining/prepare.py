"""Prepare a committed source-only overlay context; never build or deploy it."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import io


def prepare(repository,output):
    repository=Path(repository).resolve();output=Path(output).resolve()
    if output.exists():raise ValueError('new_overlay_context_required')
    def git(*args):return subprocess.check_output(['git',*args],cwd=repository)
    if git('status','--porcelain','--untracked-files=all').strip():raise ValueError('clean_committed_source_required')
    source=git('rev-parse','HEAD').decode().strip()
    archive=git('archive',source,'src','dags','docker/monitoring/run.py','docker/retraining/airflow.Dockerfile','docker/retraining/monitoring.Dockerfile')
    output.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
        for entry in stream:
            path=Path(entry.name)
            if path.is_absolute() or '..' in path.parts or not (entry.isdir() or entry.isfile()):raise ValueError('unsafe_overlay_archive')
            target=output/path
            if entry.isdir():target.mkdir(parents=True,exist_ok=True)
            else:
                target.parent.mkdir(parents=True,exist_ok=True)
                with target.open('xb') as file:shutil.copyfileobj(stream.extractfile(entry),file)
    inventory={path.relative_to(output).as_posix():hashlib.sha256(path.read_bytes()).hexdigest() for path in output.rglob('*') if path.is_file()}
    receipt={'schema_version':1,'source_git_sha':source,'files':inventory,'builds':0,'deploys':0,
        'airflow_dags_folder':'/opt/airflow/dags','requirements':'reuse supplied cached exact base images; no install layer'}
    (output/'context.json').write_text(json.dumps(receipt,sort_keys=True)+'\n')
    return receipt


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();result=prepare(args.repository,args.output)
    print(json.dumps({'outcome':'passed','source_git_sha':result['source_git_sha'],'files':len(result['files']),'builds':0,'deploys':0}))
