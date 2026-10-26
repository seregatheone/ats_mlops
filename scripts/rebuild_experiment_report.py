#!/usr/bin/env python3
"""Rebuild public tables/figures from a strict aggregate-only JSON input.

Standalone: stdlib and optional matplotlib only. No project/native/model/ledger,
network or original-data imports. Private numeric files are not accepted inputs.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re

STATES = {'not_run','incomplete','passed','pending_or_recovery_required','recovery_required'}
FAULTS = ('corrupt_data','minio_unavailable','incomplete_artifact','bad_candidate','unready_container','switch_alias_crash')
CASES = ('control','false_signal','insufficient','shifted_no_labels','shifted_labeled','replay','cooldown')
PRIVATE_PATH = re.compile(r'/Users/|/home/|/private/(?:tmp|var)/|[A-Za-z]:\\(?:Users|Documents)\\|https?://[^\s/]+:[^\s/@]+@')


def require(value, reason):
    if not value: raise ValueError(reason)


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def keys(value, allowed): require(isinstance(value,dict) and set(value)==set(allowed.split()), 'unexpected_aggregate_fields')
def count(value): require(type(value) is int and value>=0, 'invalid_aggregate_count')
def number(value, *, nullable=False, nonnegative=True):
    require(value is None and nullable or type(value) in (int,float) and math.isfinite(value) and (not nonnegative or value>=0), 'invalid_aggregate_number')


def stats(value, *, nullable=True, percentile=False):
    if value is None: require(nullable, 'missing_aggregate_statistics');return
    keys(value, 'n min mean median max' + (' p95' if percentile else ''))
    count(value['n']);require(value['n']>0, 'empty_statistics')
    for key,item in value.items():
        if key!='n': number(item)
    require(value['min']<=value['mean']<=value['max'] and value['min']<=value['median']<=value['max'], 'invalid_statistics_order')
    if percentile: require(value['min']<=value['p95']<=value['max'], 'invalid_percentile')


def validate(value):
    keys(value,'schema_version scope evidence_scope source_git_sha qualified_model_source_git_sha private_freeze_sha256 spec_sha256 private_backing_required public_projection_is_replayable overall_status series source_receipts_sha256 campaign_accounting')
    require(value['schema_version']==1 and value['scope']=='synthetic_formal_experiments' and value['evidence_scope']=='demo_only'
            and value['private_backing_required'] is True and value['public_projection_is_replayable'] is False
            and value['overall_status']=='incomplete', 'honest_synthetic_incomplete_scope_required')
    for key in ('source_git_sha','qualified_model_source_git_sha'):
        require(isinstance(value[key],str) and re.fullmatch('[0-9a-f]{40}',value[key]), 'invalid_source_sha')
    for key in ('private_freeze_sha256','spec_sha256'):
        require(isinstance(value[key],str) and re.fullmatch('[0-9a-f]{64}',value[key]), 'invalid_backing_sha')
    require(isinstance(value['source_receipts_sha256'],dict) and value['source_receipts_sha256'], 'original_receipt_hashes_required')
    for label,digest in value['source_receipts_sha256'].items():
        require(re.fullmatch('[A-Za-z0-9._-]+',label) and re.fullmatch('[0-9a-f]{64}',digest), 'unsafe_receipt_label_or_hash')
    series=value['series'];keys(series,'E1 E2 E3 E4 E5')
    for name in ('E1','E3','E4','E5'):
        row=series[name];require(row['status'] in STATES,'invalid_series_status');count(row['completed']);count(row['expected'])
        require(row['expected']=={'E1':3,'E3':18,'E4':7,'E5':1}[name] and row['completed']<=row['expected'], 'invalid_series_budget')
        require((row['status']=='passed')==(row['completed']==row['expected']), 'completion_status_mismatch')
    row=series['E1'];keys(row,'status expected completed replicas new_runs new_versions')
    require(isinstance(row['replicas'],list) and len(row['replicas'])==row['completed'], 'replica_count_mismatch')
    slots=[]
    for item in row['replicas']:
        keys(item,'slot final_fit_events forced run_cache cache_hit pairs groups max_score_abs_diff max_metric_abs_diff within_tolerance')
        require(item['slot'] in (1,2,3) and item['final_fit_events']==1 and item['forced'] is True and item['run_cache'] is False
                and item['cache_hit'] is False and type(item['within_tolerance']) is bool,'fresh_recompute_required')
        slots.append(item['slot']);count(item['pairs']);count(item['groups']);number(item['max_score_abs_diff']);number(item['max_metric_abs_diff'])
        if item['within_tolerance']:
            # Scores/metrics are in [0,1]; pairwise checks remain in the private audit.
            require(max(item['max_score_abs_diff'],item['max_metric_abs_diff'])<=1e-6+1e-5+1e-12,
                    'recompute_diff_outside_frozen_tolerance')
    require(len(slots)==len(set(slots)),'duplicate_replica_slot');count(row['new_runs']);count(row['new_versions'])
    require(row['new_runs']==row['new_versions']==row['completed'], 'independent_recompute_count_mismatch')
    if row['status']=='passed': require(all(item['within_tolerance'] for item in row['replicas']), 'recompute_tolerance_failed')
    keys(series['E2'],'status expected_pairs completed_pairs')
    require(series['E2']=={'status':'not_run','expected_pairs':5,'completed_pairs':0}, 'E2_requires_real_human_evidence')
    row=series['E3'];keys(row,'status expected completed faults')
    require(isinstance(row['faults'],list) and [x['fault'] for x in row['faults']]==list(FAULTS), 'six_faults_required')
    require(sum(item['completed'] for item in row['faults'])==row['completed'], 'fault_trial_count_mismatch')
    for item in row['faults']:
        keys(item,'fault completed expected outage_lower_seconds outage_upper_seconds detection_seconds restoration_seconds')
        count(item['completed']);require(item['expected']==3 and item['completed']<=3,'fault_repetition_budget_changed')
        for key in ('outage_lower_seconds','outage_upper_seconds','detection_seconds','restoration_seconds'):
            stats(item[key]);require((item[key] is None)==(item['completed']==0),'fault_statistics_missing')
            if item[key]: require(item[key]['n']==item['completed'],'statistics_repetitions_changed')
        if item['completed']:
            require(all(item['outage_lower_seconds'][key]<=item['outage_upper_seconds'][key]
                        for key in ('min','mean','median','max')), 'reversed_aggregate_outage_bounds')
    row=series['E4'];keys(row,'status expected completed scenarios resumed_same_request')
    require(isinstance(row['scenarios'],list) and len(row['scenarios'])==row['completed'] and len({x['scenario'] for x in row['scenarios']})==row['completed'], 'monitor_scenario_count_mismatch')
    require(row['resumed_same_request'] is None or type(row['resumed_same_request']) is bool,'invalid_resume_flag')
    for item in row['scenarios']:
        keys(item,'scenario new_requests new_runs new_versions windows minimum_window_pairs maximum_window_pairs minimum_window_vacancies maximum_window_vacancies')
        name=item['scenario'];require(name in CASES,'unknown_monitor_scenario')
        for key in ('new_requests','new_runs','new_versions','windows'):count(item[key])
        require(item['new_requests']==(1 if name=='shifted_no_labels' else 0) and item['new_runs']==item['new_versions']==(1 if name=='shifted_labeled' else 0), 'monitor_duplicate_effect')
        for key in ('minimum_window_pairs','maximum_window_pairs','minimum_window_vacancies','maximum_window_vacancies'):number(item[key],nullable=True)
    if row['status']=='passed': require(row['resumed_same_request'] is True,'verified_logical_resume_required')
    row=series['E5'];keys(row,'status expected completed metrics paired_group_bootstrap eligible_groups latency_seconds bootstrap_policy inventory_pairs inventory_groups')
    count(row['inventory_pairs']);count(row['inventory_groups'])
    require(row['bootstrap_policy']=={'bootstrap_min_groups':10,'bootstrap_replicates':2000,'bootstrap_seed':20261030,'k':10,'latency':'one_pass_per_group','warmup':'train_validation_only'}, 'frozen_E5_policy_changed')
    if row['status']!='passed': require(all(row[k] is None for k in ('metrics','paired_group_bootstrap','eligible_groups','latency_seconds')), 'uncompleted_E5_has_results')
    else:
        count(row['eligible_groups']);keys(row['metrics'],'baseline model');keys(row['latency_seconds'],'baseline model')
        keys(row['paired_group_bootstrap'],'ndcg_at_10 recall_at_10')
        for name in ('baseline','model'):
            item=row['metrics'][name];keys(item,'status ndcg_at_10 recall_at_10 eligible_group_count group_count')
            require(item['status'] in ('defined','undefined'),'invalid_quality_status')
            for key in ('ndcg_at_10','recall_at_10'):number(item[key],nullable=True);require(item[key] is None or item[key]<=1,'quality_outside_range')
            for key in ('eligible_group_count','group_count'):count(item[key])
            require(item['eligible_group_count']==row['eligible_groups'] and item['group_count']==row['inventory_groups'],'eligible_inventory_changed')
            stats(row['latency_seconds'][name],nullable=False,percentile=True)
        for metric,item in row['paired_group_bootstrap'].items():
            keys(item,'delta percentile_95 reason');number(item['delta'],nullable=True,nonnegative=False)
            baseline,model=row['metrics']['baseline'][metric],row['metrics']['model'][metric]
            expected_delta=None if baseline is None or model is None else model-baseline
            require((item['delta'] is None)==(expected_delta is None)
                    and (expected_delta is None or math.isclose(item['delta'],expected_delta,rel_tol=0,abs_tol=1e-12)),
                    'paired_delta_metric_mismatch')
            if item['percentile_95'] is None: require(item['reason']=='insufficient_eligible_groups' and row['eligible_groups']<10,'bootstrap_missing')
            else:
                require(isinstance(item['percentile_95'],list) and len(item['percentile_95'])==2 and item['reason'] is None
                        and row['eligible_groups']>=10 and expected_delta is not None,'invalid_bootstrap_interval')
                for endpoint in item['percentile_95']:
                    number(endpoint,nonnegative=False);require(-1<=endpoint<=1,'bootstrap_difference_outside_range')
                require(item['percentile_95'][0]<=item['percentile_95'][1],'reversed_bootstrap_interval')
    accounting=value['campaign_accounting']
    keys(accounting,'primary_completed_fits primary_required_fits retained_aborted_campaigns retained_completed_fits retained_successful_fault_trials retained_incomplete_precondition_trials qualification_fits total_recorded_private_fits isolated_CI_excluded')
    for key,item in accounting.items():
        if key!='isolated_CI_excluded':count(item)
    primary=series['E1']['new_runs']+sum(row['new_runs'] for row in series['E4']['scenarios'])
    require(accounting['primary_completed_fits']==primary and accounting['primary_required_fits']==4
            and accounting['retained_aborted_campaigns']==1 and accounting['retained_completed_fits']==3
            and accounting['retained_successful_fault_trials']==15 and accounting['retained_incomplete_precondition_trials']==1
            and accounting['qualification_fits']==5 and accounting['total_recorded_private_fits']==5+3+primary
            and accounting['isolated_CI_excluded'] is True,'primary_and_supplementary_fit_accounting_changed')
    require(not PRIVATE_PATH.search(canonical(value).decode()),'private_reference_in_aggregate_input')
    return value


def markdown(value):
    s=value['series'];lines=['# Synthetic formal experiments','',
        'Aggregate projection; complete validation requires the private receipts. E2 is not_run; overall study is incomplete.',
        '','| Series | Status | Completed | Expected |','|---|---|---:|---:|']
    for name,row in s.items():lines.append(f"| {name} | {row['status']} | {row.get('completed',0)} | {row.get('expected',row.get('expected_pairs'))} |")
    accounting=value['campaign_accounting']
    lines+=['','## Campaign accounting','',
        'Only the new primary campaign contributes to the experiment tables below. The previous campaign stopped on an operator import precondition before the intended crash checkpoint; all its evidence is retained separately.',
        '','| Fit scope | Count |','|---|---:|',
        f"| Primary completed fits / required | {accounting['primary_completed_fits']} / {accounting['primary_required_fits']} |",
        f"| Retained aborted campaign fits | {accounting['retained_completed_fits']} |",
        f"| Earlier qualification fits | {accounting['qualification_fits']} |",
        f"| Recorded private fits so far | {accounting['total_recorded_private_fits']} |",'',
        'The aborted campaign also retains 15 completed fault trials and one incomplete precondition failure. These are supplementary and are not merged into the primary repetitions. Isolated CI has its own lifecycle and is excluded from these fit counts.']
    lines+=['','## E1 reproducibility','','| Slot | Pairs | Groups | Maximum score difference | Maximum metric difference |','|---|---:|---:|---:|---:|']
    for row in s['E1']['replicas']:lines.append(f"| {row['slot']} | {row['pairs']} | {row['groups']} | {row['max_score_abs_diff']:.9g} | {row['max_metric_abs_diff']:.9g} |")
    lines+=['','## E3 recovery','','Finite sampling gives outage bounds, not exact downtime.','',
        '| Fault | Repetitions | Mean lower bound, s | Mean upper bound, s | Mean recovery, s |','|---|---:|---:|---:|---:|']
    for row in s['E3']['faults']:
        fmt=lambda key:'not_run' if row[key] is None else f"{row[key]['mean']:.6g}"
        lines.append(f"| {row['fault']} | {row['completed']} | {fmt('outage_lower_seconds')} | {fmt('outage_upper_seconds')} | {fmt('restoration_seconds')} |")
    lines+=['','## E4 monitoring','','| Scenario | New requests | New fits/runs | New versions |','|---|---:|---:|---:|']
    for row in s['E4']['scenarios']:lines.append(f"| {row['scenario']} | {row['new_requests']} | {row['new_runs']} | {row['new_versions']} |")
    lines+=['','## E5 final test','','Status: '+s['E5']['status']+'.']
    if s['E5']['status']=='passed':
        row=s['E5'];lines+=['','| Metric | Baseline | Frozen model | Paired delta | Percentile 95% interval |','|---|---:|---:|---:|---|']
        for metric,interval in row['paired_group_bootstrap'].items():
            lines.append(f"| {metric} | {row['metrics']['baseline'][metric]} | {row['metrics']['model'][metric]} | {interval['delta']} | {interval['percentile_95'] or interval['reason']} |")
        lines+=['','Bootstrap: 2000 paired group resamples, seed20261030; minimum10 eligible groups. Latency is one measured pass per group after train/validation-only warmup.']
    lines+=['','All data are synthetic. These results do not establish real hiring quality.','']
    return '\n'.join(lines)


def figures(value,output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size':10,'svg.hashsalt':'ats-formal-aggregate-v1'})
    def save(fig,name):
        fig.tight_layout();fig.savefig(output/(name+'.png'),dpi=180,metadata={'Software':'ATS aggregate report'})
        fig.savefig(output/(name+'.svg'),metadata={'Date':None});plt.close(fig)
    s=value['series']
    if s['E1']['replicas']:
        fig,ax=plt.subplots(figsize=(6,3));rows=s['E1']['replicas']
        labels=[str(r['slot']) for r in rows];differences=[r['max_score_abs_diff'] for r in rows]
        bars=ax.bar(labels,differences)
        ax.scatter(labels,differences,color='tab:blue',zorder=3,clip_on=False)
        ax.bar_label(bars,labels=[f'{value:.6g}' for value in differences],padding=5)
        ax.set_ylim(0,max(max(differences,default=0)*1.25,1e-5))
        ax.set(xlabel='Independent E1 fit',ylabel='Maximum score difference',title='Synthetic E1 reproducibility');save(fig,'E1-reproducibility')
    rows=[r for r in s['E3']['faults'] if r['completed']]
    if rows:
        fig,ax=plt.subplots(figsize=(9,4));ax.bar([r['fault'] for r in rows],[r['outage_upper_seconds']['mean'] for r in rows]);ax.tick_params(axis='x',rotation=25)
        ax.set(ylabel='Mean upper outage bound, seconds',title='Synthetic E3; finite HTTP sampling');save(fig,'E3-outage-bounds')
    rows=s['E4']['scenarios']
    if rows:
        fig,ax=plt.subplots(figsize=(9,4));x=list(range(len(rows)))
        ax.bar([v-.18 for v in x],[r['new_requests'] for r in rows],.36,label='New logical requests');ax.bar([v+.18 for v in x],[r['new_runs'] for r in rows],.36,label='New fits/runs')
        ax.set_xticks(x,[r['scenario'] for r in rows],rotation=25);ax.set(ylabel='Count',title='Synthetic E4 effects');ax.legend();save(fig,'E4-effects')
    if s['E5']['status']=='passed':
        fig,ax=plt.subplots(figsize=(6,3));rows=s['E5']['paired_group_bootstrap']
        for index,(metric,row) in enumerate(rows.items()):
            if row['delta'] is not None:ax.plot(index,row['delta'],'o',color='tab:blue')
            if row['percentile_95'] is not None:ax.vlines(index,*row['percentile_95'],color='tab:blue',linewidth=3)
        ax.axhline(0,color='gray',linewidth=1);ax.set_xticks(list(range(len(rows))),list(rows));ax.set(ylabel='Frozen model minus baseline',title='Synthetic E5; paired group percentile 95%');save(fig,'E5-quality-difference')
        fig,ax=plt.subplots(figsize=(6,3));rows=s['E5']['latency_seconds'];x=list(range(2))
        ax.bar([v-.18 for v in x],[rows[n]['median']*1000 for n in ('baseline','model')],.36,label='Median')
        ax.bar([v+.18 for v in x],[rows[n]['p95']*1000 for n in ('baseline','model')],.36,label='P95')
        ax.set_xticks(x,['Baseline','Frozen model']);ax.set(ylabel='Milliseconds per group',title='Synthetic E5 measured one-pass latency');ax.legend();save(fig,'E5-latency')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--output',type=Path);parser.add_argument('--figures',action='store_true');args=parser.parse_args()
    original=args.input.read_bytes();value=validate(json.loads(original))
    if args.output is None:print(json.dumps({'outcome':'aggregate_schema_validated','source_sha256':hashlib.sha256(original).hexdigest()}));return
    require(not args.output.exists(),'fresh_report_output_required');args.output.mkdir(parents=True,mode=0o700)
    (args.output/'aggregates.json').write_bytes(canonical(value));(args.output/'results.md').write_text(markdown(value))
    if args.figures:figures(value,args.output)
    manifest={'schema_version':1,'input_sha256':hashlib.sha256(original).hexdigest(),
        'private_backing_required':True,'public_projection_is_replayable':False,
        'files':{str(p.relative_to(args.output)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(args.output.rglob('*')) if p.is_file()}}
    (args.output/'manifest.json').write_bytes(canonical(manifest))
    print(json.dumps({'outcome':'report_rebuilt_from_public_aggregates','files':len(manifest['files'])+1,'input_sha256':manifest['input_sha256']}))


if __name__=='__main__':main()
