"""Verify cached local-base tag and committed context, then explicit source build."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def checked_command(context,kind,base_tag,expected_base,tag,*,run=subprocess.run):
    if not re.fullmatch(r'ats-t17-base-[a-z0-9_.-]+:[a-z0-9_.-]+',base_tag):raise ValueError('frozen_owned_local_base_tag_required')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}',expected_base):raise ValueError('exact_base_id_required')
    if not re.fullmatch(r'ats-[a-z0-9_.-]+:[a-z0-9_.-]+',tag):raise ValueError('owned_output_tag_required')
    context=Path(context).resolve();receipt=json.loads((context/'context.json').read_bytes())
    actual={path.relative_to(context).as_posix():hashlib.sha256(path.read_bytes()).hexdigest() for path in context.rglob('*') if path.is_file() and path.name!='context.json'}
    if actual!=receipt['files'] or any(path.is_symlink() for path in context.rglob('*')):raise ValueError('overlay_context_changed')
    repository=Path(__file__).resolve().parents[2]
    for name,digest in actual.items():
        blob=run(['git','show',receipt['source_git_sha']+':'+name],cwd=repository,check=True,capture_output=True).stdout
        if hashlib.sha256(blob).hexdigest()!=digest:raise ValueError('overlay_commit_mismatch')
    result=run(['docker','image','inspect','--format','{{.Id}}',base_tag],check=True,capture_output=True,text=True)
    if result.stdout.strip()!=expected_base:raise ValueError('cached_base_tag_id_mismatch')
    if kind not in {'airflow','monitoring'}:raise ValueError('overlay_kind_invalid')
    return ['docker','build','--pull=false','--network=none','--build-arg','BASE_IMAGE='+base_tag,
        '--build-arg','SOURCE_SHA='+receipt['source_git_sha'],'-f',str(context/'docker/retraining'/(kind+'.Dockerfile')),
        '-t',tag,str(context)]


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context',type=Path,required=True);parser.add_argument('--kind',choices=('airflow','monitoring'),required=True)
    parser.add_argument('--base-tag',required=True);parser.add_argument('--expected-base-id',required=True);parser.add_argument('--tag',required=True)
    parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    command=checked_command(args.context,args.kind,args.base_tag,args.expected_base_id,args.tag)
    if args.execute:subprocess.run(command,check=True)
    print(json.dumps({'outcome':'passed','kind':args.kind,'build_executed':args.execute,'deploys':0,'command':command}))
