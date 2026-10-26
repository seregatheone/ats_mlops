"""Cohort fixture record crosses the real DockerRuntime config boundary."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.evaluation.experiment import canonical, sha
from ats_mlops.evaluation.native_monitoring import cohort_intervals, fixture_launch_document
from ats_mlops.monitoring.feedback import FEATURES, export_events, load_export, utc
from ats_mlops.release.controller import DEFAULT_CONFIG
from ats_mlops.release.runtime import DockerRuntime
from ats_mlops.release.state import ReleaseError
from service.events import EventStore


class ReachedDockerBoundary(RuntimeError):pass


class MonitoringLaunchContractTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory(prefix="ats-cohort-launch-contract-")
        self.addCleanup(self.directory.cleanup);self.root=Path(self.directory.name)
        self.config=deepcopy(DEFAULT_CONFIG);self.config['port']=13001
        self.image='sha256:'+'1'*64;self.reference={'launch_path':'fixture-launch.json'}

    def save(self,value):
        (self.root/'fixture-launch.json').write_text(json.dumps(value))

    def test_campaign_document_passes_actual_runtime_config_check_before_Docker(self):
        self.save(fixture_launch_document(self.config,self.image,'release/package'))
        runtime=DockerRuntime(self.root,self.config)
        with patch('ats_mlops.release.runtime.docker',side_effect=ReachedDockerBoundary) as operation:
            with self.assertRaises(ReachedDockerBoundary):runtime.launch(self.reference)
        self.assertEqual(1,operation.call_count)
        self.assertEqual(('volume','ls','-q'),operation.call_args.args[:3])

    def test_historical_newline_encoding_fails_before_any_Docker_operation(self):
        self.save({'schema_version':1,'package_path':'release/package','image_id':self.image,
            'config_sha256':sha(canonical(self.config))})
        with patch('ats_mlops.release.runtime.docker') as operation:
            with self.assertRaisesRegex(ReleaseError,'config_changed'):DockerRuntime(self.root,self.config).launch(self.reference)
        operation.assert_not_called()

    def test_runtime_still_rejects_changed_configuration(self):
        self.save(fixture_launch_document(self.config,self.image,'release/package'))
        changed=deepcopy(self.config);changed['port']=13002
        with patch('ats_mlops.release.runtime.docker') as operation:
            with self.assertRaisesRegex(ReleaseError,'config_changed'):DockerRuntime(self.root,changed).launch(self.reference)
        operation.assert_not_called()


class MonitoringWindowClockTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory(prefix='ats-window-clock-')
        self.addCleanup(self.directory.cleanup);self.root=Path(self.directory.name)
        self.host=datetime(2026,1,1,tzinfo=timezone.utc)
        self.groups=[[f'window-{window}-{index}' for index in range(11)] for window in range(2)]
        store=EventStore(self.root/'events.sqlite3')
        clock=[self.host+timedelta(milliseconds=20+6*index) for index in range(22)]
        with patch('service.events.datetime') as service_clock:
            service_clock.now.side_effect=clock
            for group in self.groups:
                for index,identifier in enumerate(group):
                    request={'request_id':identifier,'vacancy':{'id':f'v-{index}','text':'fixture vacancy'},
                        'candidates':[{'id':f'c-{number:02}','text':'fixture candidate'} for number in range(20)],'top_k':3}
                    complete=[{'candidate_id':f'c-{number:02}','score':.5,'features':{name:.5 for name in FEATURES}}
                        for number in range(20)]
                    response=store.predict(request,lambda _:complete,{'model_version':'1','release_id':'fixture-release'})
                    self.assertEqual(identifier,response['request_id']);self.assertEqual(3,len(response['ranking']))
        self.cutoff=(self.host+timedelta(milliseconds=200)).isoformat()
        export_events(self.root/'events.sqlite3',self.root/'export',self.cutoff)
        self.events,self.manifest=load_export(self.root/'export')

    def selected(self,events,interval):
        return {event['request_id'] for event in events if utc(interval['start'])<=utc(event['event_time'])<utc(interval['end'])}

    def test_real_committed_export_keeps_all_groups_despite_host_VM_skew(self):
        # The historical host cutoff loses two ACKed requests, and its first
        # window selects only nine groups: exactly the observed failure class.
        host_end=(self.host+timedelta(milliseconds=72)).isoformat()
        host_cutoff=(self.host+timedelta(milliseconds=134)).isoformat()
        export_events(self.root/'events.sqlite3',self.root/'old-export',host_cutoff)
        old_events,_=load_export(self.root/'old-export')
        self.assertEqual(20,len(old_events))
        self.assertEqual(9,len(self.selected(old_events,{'start':self.host.isoformat(),'end':host_end})))
        self.assertEqual((22,440),(self.manifest['groups'],self.manifest['pairs']))
        intervals=cohort_intervals(self.events,self.groups,self.manifest['cutoff'])
        for interval,group in zip(intervals,self.groups):
            self.assertEqual(set(group),self.selected(self.events,interval))
            selected=[event for event in self.events if event['request_id'] in self.selected(self.events,interval)]
            self.assertEqual(11,len({event['vacancy_id'] for event in selected}))
            self.assertEqual(220,sum(len(event['candidates']) for event in selected))
        self.assertEqual(intervals[0]['end'],intervals[1]['start'])
        self.assertNotIn(self.groups[1][0],self.selected(self.events,intervals[0]))
        self.assertIn(self.groups[1][0],self.selected(self.events,intervals[1]))

    def test_missing_extra_duplicate_or_reassigned_requests_rejected(self):
        extra=deepcopy(self.events[0]);extra['request_id']='foreign-request'
        for events in (self.events[:-1],self.events+[extra],self.events+[self.events[0]]):
            with self.subTest(count=len(events)),self.assertRaisesRegex(ValueError,'E4_full_request_inventory_changed'):
                cohort_intervals(events,self.groups,self.cutoff)
        groups=deepcopy(self.groups);groups[1][0]=groups[0][0]
        with self.assertRaisesRegex(ValueError,'E4_full_request_inventory_changed'):
            cohort_intervals(self.events,groups,self.cutoff)

    def test_cross_window_ties_interleaving_and_cutoff_tie_rejected(self):
        by_id={event['request_id']:event for event in self.events}
        for stamp in (by_id[self.groups[0][0]]['event_time'],by_id[self.groups[0][-1]]['event_time']):
            events=deepcopy(self.events)
            next(event for event in events if event['request_id']==self.groups[1][0])['event_time']=stamp
            with self.subTest(stamp=stamp),self.assertRaisesRegex(ValueError,'E4_event_clock_partition_not_disjoint'):
                cohort_intervals(events,self.groups,self.cutoff)
        last=by_id[self.groups[1][-1]]['event_time']
        for cutoff in (last,(utc(last)-timedelta(microseconds=1)).isoformat()):
            with self.subTest(cutoff=cutoff),self.assertRaisesRegex(ValueError,'E4_event_clock_partition_not_disjoint'):
                cohort_intervals(self.events,self.groups,cutoff)

    def test_ties_inside_one_window_preserve_the_full_group_partition(self):
        events=deepcopy(self.events);by_id={event['request_id']:event for event in events}
        by_id[self.groups[0][1]]['event_time']=by_id[self.groups[0][0]]['event_time']
        intervals=cohort_intervals(events,self.groups,self.cutoff)
        for interval,group in zip(intervals,self.groups):self.assertEqual(set(group),self.selected(events,interval))


if __name__=='__main__':unittest.main()
