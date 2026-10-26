"""Minimal T18 preflight/materialization. No build, service, fit or test-body read.

Root explicitly executes materialize only after source review. It creates a clean
source clone, copies the admitted train/validation carrier and writes private
S1 inputs/authorization/commands. S1 itself is a separate explicit runtime action.
"""
from copy import deepcopy
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import subprocess
import sys

from ats_mlops.evaluation import native
from ats_mlops.evaluation.experiment import canonical,immutable,read,require,sha,validate_spec
from ats_mlops.pipeline import verify_bundle
from tests.e2e.audit import derive_holdout_lineage

PROJECT='ats-acceptance-t18a'
PORTS={'minio':19000,'console':19001,'mlflow':15050,'api':13000,'airflow':28080,'fixture':13001}
ML_IMAGE='sha256:574c48deb107cba3f0bdc982d3aa0ab1500de58ab1694052bb92efbcab13ec91'
CORE_IMAGES={'postgres':'ats-core-postgres:t4','minio':'ats-core-minio:source9e49d5e','minio-init':'ats-core-mc:source7394ce0','mlflow':ML_IMAGE}
DEFAULT_ROOT=Path.home()/'.local/state/ats-mlops-evidence/t18/attempt-001'
DEFAULT_BUNDLE=Path.home()/'.local/state/ats-mlops-evidence/t17/attempt-002-passed-48b6d3a/actual/bundle-v2'
CHILD_ROOT=Path.home()/'.local/state/ats-mlops-evidence/t18/preflight/child-002-64groups'
CHILD_RECEIPT='9972480b4416e1db51d9a21e1a383b27816f877b05c9451bd0ae485634fdb9a5'


def inspect_image(reference):
    value=json.loads(subprocess.check_output(['docker','image','inspect','--format',
        '{"id":{{json .Id}},"repo_digests":{{json .RepoDigests}},"architecture":{{json .Architecture}},"os":{{json .Os}},"revision":{{json (index .Config.Labels "org.opencontainers.image.revision")}}}',reference]))
    require(re.fullmatch(r'sha256:[0-9a-f]{64}',value['id']) and value['os']=='linux','cached_Linux_image_required')
    return dict(value,reference=reference)


def free_ports(ports):
    require(len(set(ports.values()))==len(ports) and all(type(port) is int and 10000<=port<=65535 for port in ports.values()),'private_distinct_ports_required')
    for port in ports.values():
        with socket.socket() as probe:
            probe.settimeout(.2)
            require(probe.connect_ex(('127.0.0.1',port))!=0,'private_port_busy_leave_owner_untouched')


def preflight(repository,source,root,bundle,holdout,serving_state):
    require(Path(holdout).name=='data_manifest.json','holdout_manifest_metadata_required')
    require(all(not Path(path).is_symlink() and Path(path).absolute()==Path(path).resolve()
                for path in (repository,root,bundle,holdout)),'canonical_preparation_paths_required')
    repository,root,bundle,holdout=map(lambda path:Path(path).resolve(),(repository,root,bundle,holdout))
    native.source_guard(repository,source)
    require(not root.exists() and not root.is_symlink(),'fresh_private_attempt_root_required')
    tracked=subprocess.check_output(['git','ls-tree','-r','--name-only',source],cwd=repository,text=True).splitlines()
    require(not any(Path(name).name in {'test.jsonl','corpus.jsonl','source.jsonl','split.json'} for name in tracked),'reserved_body_in_Git_source')
    expected=read(repository/'reports/retraining/t17/lineage/bundle-descriptor.json')['sha256']
    descriptor=verify_bundle(bundle,expected)
    manifest=read(bundle/'data_manifest.json')
    require(type(manifest['schema_version']) is int and manifest['schema_version']==2
        and manifest['generator']['config']['seed']==20261027 and manifest['generator']['config']['vacancy_groups']==60,'admitted_v2_fixture_required')
    require(not any(Path(name).name in {'test.jsonl','corpus.jsonl','source.jsonl','split.json'} for name in descriptor['files']),'unsafe_carrier')
    policy=read(bundle/manifest['reservation']['policy']['path']);lineage=derive_holdout_lineage(policy)
    held=read(holdout)  # METADATA only. Never stat/open/hash the test artifact path.
    require('test' in held['splits'] and held['artifacts']['test']['sha256'],'holdout_metadata_missing')
    child_bytes=(CHILD_ROOT/'child-preflight.json').read_bytes();child=read(CHILD_ROOT/'child-preflight.json');child_plan=read(CHILD_ROOT/'plan.json')
    require(sha(child_bytes)==CHILD_RECEIPT and child['outcome']=='admitted_new_training_labels'
        and type(child['new_training_semantic_labels']) is int and child['new_training_semantic_labels']==40
        and child['parent_bundle_sha256']==expected and child_plan['config']['vacancy_groups']==64
        and child_plan['config']['namespace']=='ats-t18-e4-child-v2','accepted_structural_child_required')
    free_ports(PORTS)
    for kind,argv in (('containers',['docker','ps','-aq']),('volumes',['docker','volume','ls','-q']),('networks',['docker','network','ls','-q'])):
        require(not subprocess.check_output([*argv,'--filter','label=com.docker.compose.project='+PROJECT]).strip(),'private_namespace_already_exists')
    images={role:inspect_image(reference) for role,reference in CORE_IMAGES.items()}
    components={role:inspect_image(reference) for role,reference in {'serving':'2d2b','airflow_base':'9703','monitoring_base':'43d5'}.items()}
    # Read the existing default journal only for its generic serving component;
    # do not observe an API, change aliases or copy its model/ACTIVE identity.
    from ats_mlops.release.state import Store
    store=Store.__new__(Store);store.root=Path(serving_state)
    state=store.read();active=state['active'];require(active is not None,'generic_serving_component_missing')
    serving=read(store.root/active['package_path']/'release_manifest.json')['serving']
    from service.integrity import inventory
    require(serving['image']['id']==components['serving']['id'] and serving['source_sha256']==inventory(repository/'service'),'cached_serving_source_mismatch')
    return {'schema_version':1,'outcome':'prepared_plan_only','source_git_sha':source,'origin_repository':str(repository),'root':str(root),
        'project':PROJECT,'ports':PORTS,'bundle':str(bundle),'bundle_sha256':expected,'carrier_files':sorted(descriptor['files']),
        'holdout_manifest':str(holdout),'holdout_manifest_sha256':sha(holdout.read_bytes()),'holdout_lineage':lineage,
        'child':{'root':str(CHILD_ROOT),'receipt_sha256':CHILD_RECEIPT,'receipt':child,'plan':child_plan},
        'images':images,'components':components,'serving':serving,'effects':{'builds':0,'services':0,'fits':0,'test_body_reads':0,'real_ledger_opens':0}}


def save_private(path,value):
    immutable(path,value);Path(path).chmod(0o600)


def materialize(plan):
    root=Path(plan['root']);root.mkdir(parents=True,mode=0o700)
    save_private(root/'preparation.json',plan)
    source=root/'source'
    subprocess.run(['git','clone','--no-local','--no-hardlinks','--no-checkout',plan['origin_repository'],str(source)],check=True,capture_output=True)
    subprocess.run(['git','checkout','--detach',plan['source_git_sha']],cwd=source,check=True,capture_output=True)
    require(not subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],cwd=source).strip(),'clean_source_clone_required')
    carrier=root/'inputs/v2';carrier.mkdir(parents=True)
    for name in [*plan['carrier_files'],'bundle.json']:
        original=Path(plan['bundle'])/name
        require(not original.is_symlink(),'carrier_symlink_forbidden')
        target=carrier/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(original,target)
    verify_bundle(carrier,plan['bundle_sha256'])
    metadata=read(carrier/'data_manifest.json');held=read(plan['holdout_manifest']);policy=carrier/metadata['reservation']['policy']['path']
    identity=root/'inputs/serving-identity.json';save_private(identity,plan['serving'])
    structural=plan['child'];accepted=structural['receipt'];original_plan=structural['plan']
    child_plan={"schema_version":1,"source_git_sha":plan['source_git_sha'],"generation_source_git_sha":accepted['generation_source_git_sha'],
        "seed":20261027,"config":original_plan['config'],"config_sha256":accepted['config_sha256'],
        "parent_manifest_sha256":accepted['parent_manifest_sha256'],"parent_bundle_sha256":accepted['parent_bundle_sha256'],
        "parent_policy_sha256":accepted['parent_policy_sha256'],"root_policy_sha256":plan['holdout_lineage']['root_policy_sha256'],
        "accepted_preflight_sha256":structural['receipt_sha256'],"child_bundle_sha256":accepted['child_bundle_sha256'],
        "approved_at":original_plan['approved_at'],"bundle_uri":original_plan['bundle_uri'],"structural_revision":original_plan['structural_revision']}
    save_private(root/'inputs/accepted-child-plan.json',child_plan)
    model={'package':str(root/'S1/package'),'check':{'path':str(root/'S1/release-check.json'),'sha256':None},
        'training_manifest':{'path':str(carrier/'data_manifest.json'),'sha256':sha((carrier/'data_manifest.json').read_bytes())},
        'admission_report':{'path':str(carrier/'admission/data-admission.json'),'sha256':sha((carrier/'admission/data-admission.json').read_bytes())},
        'dataset_binding':{'path':str(carrier/'dataset-binding.json'),'sha256':sha((carrier/'dataset-binding.json').read_bytes())},
        'selection':{'path':str(root/'S1/qualified-selection.json'),'sha256':None},'plan':{'path':str(root/'S1/qualification-plan.json'),'sha256':None}}
    config={'schema_version':1,'project':plan['project'],'repo':str(source),'bundle':str(carrier),'bundle_sha256':plan['bundle_sha256'],
        'image':plan['images']['mlflow'],'network':plan['project']+'_default','tracking_uri':'http://mlflow:5000','threads':1,'worker_memory_mib':1536,
        'release':{'port':plan['ports']['api'],'registry_uri':'http://127.0.0.1:'+str(plan['ports']['mlflow']),'model_name':'ats-pair-ranking','ready_timeout_seconds':60},
        'core':{'containers':{name:plan['project']+'-'+name+'-1' for name in ('postgres','minio','mlflow')},
            'ports':{name:plan['ports'][name] for name in ('minio','console','mlflow')},'images':{role:value['id'] for role,value in plan['images'].items()}},
        'fixtures':{'serving_identity':{'path':str(identity),'sha256':sha(identity.read_bytes())}},'qualified':model,
        'holdout':{'manifest':{'path':plan['holdout_manifest'],'sha256':plan['holdout_manifest_sha256']},'policy':{'path':str(policy),'sha256':sha(policy.read_bytes())}},
        'E4':None}
    environment=root/'environment-S1.json';save_private(environment,config)
    value=native.wire_adapters(read(source/'configs/experiment.template.json'))
    value['source_git_sha']=plan['source_git_sha'];bindings=value['bindings']
    bindings.update(environment_sha256=sha(environment.read_bytes()),data_manifest_sha256=sha((carrier/'data_manifest.json').read_bytes()),
        admission_sha256=sha((carrier/'admission/data-admission.json').read_bytes()),split_sha256=metadata['artifacts']['split_lists']['sha256'],
        protocol_sha256=sha((source/'docs/experiment-protocol.md').read_bytes()),test_partition_sha256=held['artifacts']['test']['sha256'],
        **{key:plan['holdout_lineage'][key] for key in ('root_policy_sha256','protected_test_documents_sha256')})
    _,ledger=native.holdout_ledger();bindings['holdout_ledger_sha256']=sha(canonical(ledger))
    test=held['splits']['test'];value['test_inventory']={'pairs':test['rows'],'groups':test['groups'],
        'group_ids_sha256':sha(canonical(sorted(test['vacancy_ids']))),'pair_ids_sha256':sha(canonical(sorted(test['pair_ids'])))}
    validate_spec(value,template=True)
    save_private(root/'spec-S1.json',value)
    auth={'schema_version':1,'scope':'T18_private_qualification','actor':'root-coordinator','source_git_sha':plan['source_git_sha'],
        'project':plan['project'],'input_spec_sha256':sha(canonical(value)),'phases':['S1']}
    save_private(root/'authorization-S1.json',auth)
    # Source-only contexts are prepared from Git, not from ignored host files.
    subprocess.run([sys.executable,'-B',source/'docker/retraining/prepare.py','--repository',source,'--output',root/'overlay-context'],check=True,capture_output=True)
    overlay=[]
    for role in ('airflow','monitoring'):
        base=plan['components'][role+'_base']['id'];base_tag='ats-t17-base-t18-'+role+':'+plan['source_git_sha'][:10];tag='ats-t18-'+role+':'+plan['source_git_sha'][:10]
        overlay.extend([['docker','tag',base,base_tag],[sys.executable,'-B',str(source/'docker/retraining/build.py'),'--context',str(root/'overlay-context'),
            '--kind',role,'--base-tag',base_tag,'--expected-base-id',base,'--tag',tag,'--execute']])
    save_private(root/'overlay-commands.json',{'source_git_sha':plan['source_git_sha'],'commands':overlay,'executed':False,'no_installs':True,'services_started':0})
    argv=[sys.executable,'-B','-m','tests.e2e.driver','S1','--spec',str(root/'spec-S1.json'),'--output',str(root),'--authorization',str(root/'authorization-S1.json')]
    runner='#!/bin/sh\nset -eu\ncd '+shlex.quote(str(source))+'\nexport PYTHONPATH='+shlex.quote(str(source/'src'))+'\nexport ATS_EXPERIMENT_ENVIRONMENT='+shlex.quote(str(environment))+'\nexec '+shlex.join(argv)+'\n'
    with (root/'run-S1.sh').open('x') as stream:stream.write(runner)
    (root/'run-S1.sh').chmod(0o700)
    save_private(root/'materialized.json',{'outcome':'prepared_only','source_git_sha':plan['source_git_sha'],'spec_sha256':sha(canonical(value)),
        'bundle_sha256':plan['bundle_sha256'],'test_body_read':False,'builds':0,'services':0,'fits':0,'real_ledger_created':False,
        'next':'root reviews exact inputs and opens runtime window, then explicitly runs run-S1.sh; no auto retry after first fit'})
    return {'outcome':'prepared_only','source_git_sha':plan['source_git_sha'],'root':str(root),'builds':0,'services':0,'fits':0,'test_body_reads':0}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=('preflight','materialize'))
    parser.add_argument('--repository',type=Path,required=True);parser.add_argument('--source-sha',required=True)
    parser.add_argument('--root',type=Path,default=DEFAULT_ROOT);parser.add_argument('--bundle',type=Path,default=DEFAULT_BUNDLE)
    parser.add_argument('--holdout-manifest',type=Path,required=True)
    parser.add_argument('--serving-state',type=Path,default=Path.home()/'.local/state/ats-mlops')
    args=parser.parse_args(argv)
    try:
        plan=preflight(args.repository,args.source_sha,args.root,args.bundle,args.holdout_manifest,args.serving_state)
        result=materialize(plan) if args.action=='materialize' else {'outcome':'preflight_only','plan_sha256':sha(canonical(plan)),
            'source_git_sha':plan['source_git_sha'],'bundle_sha256':plan['bundle_sha256'],'ports':plan['ports'],'effects':plan['effects']}
        print(json.dumps(result,sort_keys=True));return 0
    except Exception as error:
        reason=str(error) if isinstance(error,native.ExperimentError) and re.fullmatch('[A-Za-z_]+',str(error)) else 'T18_preparation_failed'
        print(json.dumps({'outcome':'failed','reason':reason}));return 1

if __name__=='__main__':raise SystemExit(main())
