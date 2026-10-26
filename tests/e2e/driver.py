"""Explicit T18 qualification entrypoints; import/recipe have no runtime effects.

Runtime phases require a root-coordinator authorization for this exact source,
project and input-spec hash. E5 is intentionally absent. Evidence collection is
read-only with respect to the system and writes only a new private bundle.
"""
from datetime import datetime,timezone
from pathlib import Path
import argparse
import json
import os
import re
import subprocess

from ats_mlops.evaluation import native
from ats_mlops.evaluation.experiment import canonical,decode,immutable,read,require,sha,validate_spec,FAULTS
from tests.e2e.audit import SCENARIOS

PHASES=("S1","S2","negative-fixture","schema","managed","registry",*("fault-"+name for name in FAULTS),
        "monitoring-prepare",*("monitoring-"+name for name in ("control","false_signal","insufficient","shifted_no_labels","shifted_labeled","replay","cooldown")),
        "stop-startup-api")


def authorize(spec,path,phase):
    value=read(path)
    require(isinstance(value,dict) and set(value)=={"schema_version","scope","actor","source_git_sha","project","input_spec_sha256","phases"}
        and type(value["schema_version"]) is int and value["schema_version"]==1 and value["scope"]=="T18_private_qualification"
        and value["actor"]=="root-coordinator" and value["source_git_sha"]==spec["source_git_sha"]
        and value["input_spec_sha256"]==sha(canonical(spec)) and isinstance(value["phases"],list)
        and set(value["phases"])<=set(PHASES) and phase in value["phases"],"exact_qualification_authorization_required")
    # Validation checks the complete source/adapter binding before any runtime
    # callback. S1 alone accepts the declared pre-model qualification draft.
    series="E4" if phase.startswith("monitoring-") else "E1" if phase in {"S1","S2","stop-startup-api"} else "E3"
    config=native.environment(spec,series=series,qualification=phase=="S1")
    require(value["project"]==config["project"],"qualification_project_mismatch")
    return config,value


def phase(spec_path,output,authorization,action):
    require(action in PHASES,"unknown_qualification_phase")
    spec=read(spec_path);config,grant=authorize(spec,authorization,action)
    output=Path(output);started=datetime.now(timezone.utc).isoformat()
    # Each phase is one explicit attempt. Never replace failed evidence or reset
    # persistent policy/request IDs to make a repeated invocation look fresh.
    receipt_dir=output/"phase-receipts";receipt_dir.mkdir(parents=True,exist_ok=True,mode=0o700)
    intent=receipt_dir/(action+"-intent.json")
    immutable(intent,{"schema_version":1,"phase":action,"source_git_sha":spec["source_git_sha"],"scope":"T18_qualification_not_E1_E5",
        "input_spec_sha256":sha(canonical(spec)),"authorization_sha256":sha(canonical(grant)),"started_at":started})
    try:
        if action=="S1":
            root=output/"S1";root.mkdir();result=native.clean_start(spec,root)
        elif action=="S2":
            root=output/"S2";root.mkdir();result=[]
            for index in range(1,4):
                replica=root/str(index);replica.mkdir();result.append(native.e1(spec,replica))
        elif action.startswith("fault-"):
            root=output/"faults"/(action.removeprefix("fault-")+"-1");root.mkdir(parents=True)
            result=native.e3(spec,root)
        elif action.startswith("monitoring-"):
            from ats_mlops.evaluation.native_monitoring import ORDER
            scenario=action.removeprefix("monitoring-")
            if scenario=="prepare":result=native.prepare_monitoring(spec,Path(config["E4"]["root"]))
            else:
                require(scenario in ORDER,"unknown_monitoring_qualification_phase")
                root=output/"monitoring-attempts"/scenario;root.mkdir(parents=True);result=native.e4(spec,root)
                immutable(root/"observations.json",result["observations"])
        elif action=="stop-startup-api":
            root=output/"startup-cleanup";root.mkdir();commands=native.Commands(root)
            native.owned_core(commands,config)
            controller=native._controller(config,output/"S1")
            native._cleanup_release(controller,commands)
            result={"outcome":"owned_startup_API_stopped","core_retained":True,"commands_sha256":commands.finish()}
        else:
            function={"negative-fixture":native.degraded_fixture,"schema":native.schema_rejection,
                      "managed":native.managed_release,"registry":native.registry_outage}[action]
            root=output/action;root.mkdir();result=function(spec,root)
        receipt={"schema_version":1,"outcome":"completed","phase":action,"scope":"T18_qualification_not_E1_E5",
            "source_git_sha":spec["source_git_sha"],"input_spec_sha256":sha(canonical(spec)),"started_at":started,
            "finished_at":datetime.now(timezone.utc).isoformat(),"result":result}
        immutable(receipt_dir/(action+".json"),receipt)
        return receipt
    except BaseException as error:
        immutable(receipt_dir/(action+"-failed.json"),{"schema_version":1,"outcome":"failed","phase":action,
            "source_git_sha":spec["source_git_sha"],"started_at":started,"finished_at":datetime.now(timezone.utc).isoformat(),
            "error_type":type(error).__name__,"scope":"preserved_failed_qualification_attempt"})
        raise


class Collector:
    """Explicit allowlist capture; never recursively copy a workspace/core/logdir."""
    def __init__(self,root):
        self.root=Path(root)
        require(self.root.is_absolute() and self.root==self.root.resolve(),'canonical_capture_root_required')
        self.root.mkdir(parents=True,exist_ok=False,mode=0o700);self.index={}

    def target(self,name):
        relative=Path(name)
        require(not relative.is_absolute() and relative.as_posix()==name and relative.parts
                and all(part not in ('..','.') for part in relative.parts),'unsafe_evidence_capture')
        current=self.root
        for part in relative.parts[:-1]:
            current=current/part;require(not current.is_symlink(),'capture_symlink_forbidden')
            current.mkdir(exist_ok=True)
        target=self.root/relative
        require(not target.exists() and not target.is_symlink(),'capture_already_exists')
        return target

    def copy(self,path,name):
        path=Path(path);relative=Path(name)
        require(not relative.is_absolute() and '..' not in relative.parts and relative.name not in
                {'test.jsonl','source.jsonl','corpus.jsonl','split.json'},"unsafe_evidence_capture")
        require(path.suffix!='.env' and relative.suffix!='.env','private_credentials_capture_forbidden')
        current=path
        while current!=current.parent:
            require(not current.is_symlink(),"capture_symlink_forbidden");current=current.parent
        require(path.is_file(),"capture_input_missing")
        require(name not in self.index,"duplicate_capture_name");destination=self.target(name)
        content=path.read_bytes()
        with destination.open('xb') as stream:stream.write(content);stream.flush();os.fsync(stream.fileno())
        reference={'path':relative.as_posix(),'sha256':sha(content)};self.index[name]=reference
        return reference

    def json(self,name,value):
        path=self.target(name);immutable(path,value)
        reference={'path':name,'sha256':sha(canonical(value))};self.index[name]=reference
        return reference

    def commands(self,case,prefix):
        value=read(Path(case)/'commands.json');result=[]
        for index,command in enumerate(value['commands'],1):
            allowed=command['accepted_exit_codes']
            require(isinstance(allowed,list) and allowed and all(type(code) is int for code in allowed)
                    and type(command['exit_code']) is int and command['exit_code'] in allowed,'unexpected_native_command_exit')
            result.append({'id':prefix+'-'+str(index),'argv':command['argv'],'exit_code':command['exit_code'],
                'expected_exit_code':command['exit_code'],'accepted_exit_codes':allowed,
                **{key:self.copy(Path(case)/command[key]['path'],prefix+'/'+command[key]['path']) for key in ('stdout','stderr')}})
        require(bool(result),'actual_command_log_required')
        return result

    def envelope(self,scenario,spec,started,finished,native_refs,commands,*,execution_source=None,phase_attestations=None):
        require(scenario in SCENARIOS,'unknown_scenario')
        actual_source=spec['source_git_sha'] if execution_source is None else execution_source
        require(isinstance(actual_source,str) and re.fullmatch('[0-9a-f]{40}',actual_source),'invalid_actual_execution_source')
        attestations=[] if phase_attestations is None else phase_attestations
        require(isinstance(attestations,list),'invalid_phase_attestations')
        value={'schema_version':1,'outcome':'passed','execution_kind':'actual_acceptance','evidence_scope':'demo_only',
            'source_git_sha':actual_source,'spec_sha256':sha(canonical(spec)),'started_at':started,'finished_at':finished,
            'scenario':scenario,'name':SCENARIOS[scenario][0],'commands':commands,'native':native_refs,'phase_attestations':attestations}
        return self.json('scenarios/'+scenario+'.json',value)

    def source(self,repo,source_sha,files):
        native.source_guard(repo,source_sha)
        results={}
        for name,args in (('head',['rev-parse','HEAD']),('status',['status','--porcelain','--untracked-files=all'])):
            run=subprocess.run(['git',*args],cwd=repo,capture_output=True,check=True)
            target=self.root/'source'/name;target.parent.mkdir(exist_ok=True)
            with target.open('xb') as stream:stream.write(run.stdout);stream.flush();os.fsync(stream.fileno())
            results[name]={'path':'source/'+name,'sha256':sha(run.stdout)}
        results['files']={name:self.copy(Path(repo)/name,'source/files/'+name) for name in files}
        return results

    def startup(self,raw):
        raw=Path(raw);workspace=raw/'run/workspace';final=workspace/'artifacts/training/final'
        inputs={'startup':raw/'startup.json','plan':raw/'qualification-plan.json','selection':raw/'qualified-selection.json',
            'run':final/'run_manifest.json','pipeline':workspace/'artifacts/evaluation/pipeline_manifest.json',
            'features':workspace/'artifacts/evaluation/features.json','gate':raw/'model-gate.json','package':raw/'package/release_manifest.json',
            'check':raw/'release-check.json','journal':raw/'release-state/journal.jsonl','isolation':raw/'isolation.json'}
        result={key:self.copy(path,'native/S1/'+key+('.jsonl' if key=='journal' else '.json')) for key,path in inputs.items()}
        manifest=read(inputs['package'])
        result['package_files']={name:self.copy(raw/'package'/name,'native/S1/package/'+name) for name in manifest['files']}
        return result

    def replicas(self,raw):
        result=[]
        for index in range(1,4):
            base=Path(raw)/str(index);workspace=base/'run/workspace';evaluation=workspace/'artifacts/evaluation'
            inputs={'repro':base/'repro.json','pipeline':evaluation/'pipeline_manifest.json','run':workspace/'artifacts/training/final/run_manifest.json',
                'features':evaluation/'features.json','scores':evaluation/'scores.json','metrics':evaluation/'metrics.json'}
            result.append({'workspace_id':read(base/'repro.json')['invocation'],**{key:self.copy(path,f'native/S2/{index}/{key}.json') for key,path in inputs.items()}})
        return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('phase',choices=('recipe',*PHASES))
    parser.add_argument('--spec',type=Path);parser.add_argument('--output',type=Path);parser.add_argument('--authorization',type=Path)
    args=parser.parse_args(argv)
    if args.phase=='recipe':
        print(json.dumps({'outcome':'prepared_only','effects':0,'phases':list(PHASES),
            'manual_setup':'fresh private core config/images and reviewed private Airflow/source overlay; no default service stops',
            'E5':'absent; only frozen native evaluate_e5 after separate explicit authorization'}));return 0
    try:
        require(all(value is not None for value in (args.spec,args.output,args.authorization)),'qualification_inputs_required')
        result=phase(args.spec,args.output,args.authorization,args.phase)
        print(json.dumps({'outcome':result['outcome'],'phase':args.phase,'source_git_sha':result['source_git_sha']}));return 0
    except Exception:
        print('{"outcome":"failed","reason":"qualification_failed"}');return 1

if __name__=='__main__':raise SystemExit(main())
