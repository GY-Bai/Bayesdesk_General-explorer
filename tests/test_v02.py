"""Security, recovery and long-term knowledge regressions introduced in V0.2."""
from __future__ import annotations
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from bayesdesk.broker import Broker
from bayesdesk.leader import Leader
from bayesdesk.errors import ContractError
from bayesdesk.contracts import sign_permit
from bayesdesk.storage import connect, dumps, write_tx
from bayesdesk.recipes import materialize_source

SECRET = 'v02-test-secret-requires-32-bytes+'
SHA = 'a'*40
CPU = {'cpu_units':2,'memory_mib':1024,'gpu_count':0}
GPU = {'cpu_units':2,'memory_mib':1024,'gpu_count':1}
INPUTS = {'duration':0,'result':'pass'}
POLICY = {'recipes':{'smoke.v1':{
    'profiles':{'cpu':CPU,'gpu':GPU},
    'inputs':{'duration':{'type':'integer','min':0,'max':30},'result':{'type':'enum','choices':['pass','fail']}},
    'max_timeout_seconds':30,'max_attempts':3,
}}}
ACCEPT={'type':'job_exit','exit_code':0}

class SecurityAndRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        d=Path(self.tmp.name)
        self.leader=Leader(d/'leader.sqlite3',SECRET)
        self.broker=Broker(d/'broker.sqlite3', {'node_id':'node', 'allocatable':{'cpu_units':4,'memory_mib':4096,'gpu_count':1}},
                           {'smoke.v1': {'argv':[sys.executable,'-m','bayesdesk.simjob'],
                            'cwd':str(Path(__file__).resolve().parents[1]),
                            'parameters':{'duration':{'type':'integer','flag':'--duration','min':0,'max':30},
                                          'result':{'type':'enum','flag':'--result','choices':['pass','fail']}}}},
                           SECRET,d/'jobs','local')
        self.leader.approve_decision('D1','operator','smoke approval')
        self.leader.create_task('T1','D1','run smoke',[],[],ACCEPT,execution_policy=POLICY,knowledge_scope={'gpu':'GTX1060'})

    def prep(self, task='T1', worker='w1', inputs=None, profile='cpu', timeout=10):
        a=self.leader.assign(worker)
        self.assertEqual(a['task_id'],task)
        input_values=inputs or INPUTS
        h=self.leader.prepare_handoff(task,worker,a['generation'],'smoke.v1',SHA,{profile:POLICY['recipes']['smoke.v1']['profiles'][profile]},
                                       timeout,inputs=input_values)
        j={'schema_version':1,'handoff_id':h['handoff_id'],'task_id':task,'attempt_id':h['attempt_id'],
           'decision_id':h['decision_id'],'generation':h['generation'],'source_commit':SHA,'recipe_id':'smoke.v1',
           'inputs':dict(input_values),'profile_name':profile,'profile':dict(POLICY['recipes']['smoke.v1']['profiles'][profile]),
           'timeout_seconds':timeout,'permit':h['permit']}
        return a,h,j

    def finish(self,h,j):
        got=self.broker.submit(j)
        self.leader.acknowledge_handoff(h['handoff_id'],got['receipt'])
        self.broker.dispatch()
        for _ in range(100):
            self.broker.reconcile()
            if self.broker.status(got['job_id'])['state'] in ('SUCCEEDED','FAILED'):break
            time.sleep(.05)
        self.assertEqual(self.broker.status(got['job_id'])['state'],'SUCCEEDED')
        self.leader.ingest_event(self.broker.outbox()[0])
        return got

    def test_input_signed_against_mutation(self):
        _,h,j=self.prep()
        j['inputs']['result']='fail'
        with self.assertRaises(ContractError) as e: self.broker.submit(j)
        self.assertEqual(e.exception.code,'INPUT_NOT_APPROVED')

    def test_policy_blocks_unapproved_recipe_profiles_walltime_and_inputs(self):
        a=self.leader.assign('w1')
        args=('T1','w1',a['generation'])
        cases=[('unapproved',{'cpu':CPU},10,INPUTS,'RECIPE_NOT_APPROVED'),
               ('smoke.v1',{'other':GPU},10,INPUTS,'PROFILE_NOT_APPROVED'),
               ('smoke.v1',{'cpu':CPU},99,INPUTS,'TIMEOUT_NOT_APPROVED'),
               ('smoke.v1',{'cpu':CPU},10,{'duration':300,'result':'pass'},'INPUT_NOT_APPROVED')]
        for recipe,profiles,timeout,inputs,expected in cases:
            with self.subTest(expected=expected),self.assertRaises(ContractError) as e:
                self.leader.prepare_handoff(*args,recipe,SHA,profiles,timeout,inputs=inputs)
            self.assertEqual(e.exception.code,expected)

    def test_broker_receipt_must_authenticate_accepted_handoff(self):
        _,h,j=self.prep()
        got=self.broker.submit(j)
        with self.assertRaises(ContractError): self.leader.acknowledge_handoff(h['handoff_id'],{'job_id':'fake'})
        forged=dict(got['receipt']);forged['job_id']='JOB-forged'
        with self.assertRaises(ContractError): self.leader.acknowledge_handoff(h['handoff_id'],forged)
        self.assertEqual(self.leader.task('T1')['status'],'HANDOFF_PREPARED')
        self.leader.acknowledge_handoff(h['handoff_id'],got['receipt'])
        self.assertEqual(self.leader.task('T1')['status'],'WAITING_JOB')

    def test_ack_lost_reconciler_recovers(self):
        _,h,j=self.prep()
        got=self.broker.submit(j)
        self.assertEqual(self.leader.reconcile_handoffs(self.broker)['recovered'],[h['handoff_id']])
        self.assertEqual(self.leader.task('T1')['last_job_id'],got['job_id'])
        self.assertEqual(self.leader.reconcile_handoffs(self.broker)['recovered'],[])

    def test_local_coordinator_reconciles_lost_ack_and_terminal_event(self):
        from bayesdesk.coordinator import LocalCoordinator
        _,handoff,job=self.prep()
        accepted=self.broker.submit(job)
        self.broker.dispatch()
        for _ in range(100):
            self.broker.reconcile()
            if self.broker.status(accepted['job_id'])['state']=='SUCCEEDED':break
            time.sleep(.03)
        self.assertEqual(self.broker.status(accepted['job_id'])['state'],'SUCCEEDED')
        service=LocalCoordinator(self.leader,self.broker)
        actual=service.tick()
        self.assertEqual(actual['handoffs']['recovered'],[handoff['handoff_id']])
        self.assertEqual(actual['events_delivered'],1)
        self.assertEqual(self.leader.task('T1')['status'],'RESULT_READY')
        self.assertEqual(service.tick()['events_delivered'],0)

    def test_prepared_missing_not_replayed_unless_expired_and_confirmed_absent(self):
        _,h,j=self.prep()
        self.assertEqual(self.leader.reconcile_handoffs(self.broker)['pending'],[h['handoff_id']])
        with connect(self.leader.path) as db, write_tx(db):
            original=json.loads(db.execute('SELECT permitted_json FROM handoffs WHERE id=?',(h['handoff_id'],)).fetchone()[0])
            original['claims']['expires_at']=int(time.time())-180
            db.execute('UPDATE handoffs SET permitted_json=? WHERE id=?',
                       (dumps(sign_permit(SECRET,original['claims'])),h['handoff_id']))
        self.assertEqual(self.leader.reconcile_handoffs(self.broker)['aborted'],[h['handoff_id']])
        self.assertEqual(self.leader.task('T1')['status'],'READY')

    def test_job_acceptance_rejects_fabricated_or_failed_results(self):
        a=self.leader.assign('w1')
        with self.assertRaises(ContractError): self.leader.complete('T1','w1',a['generation'],['not-registered'])
        ev=self.leader.record_evidence('T1','w1',a['generation'],'test_report','some test passed')
        with self.assertRaises(ContractError) as e: self.leader.complete('T1','w1',a['generation'],[ev['evidence_id']])
        self.assertEqual(e.exception.code,'ACCEPTANCE_FAILED')

    def test_job_result_can_complete_after_new_worker_claim(self):
        _,h,j=self.prep()
        self.finish(h,j)
        a=self.leader.assign('w2')
        self.leader.complete('T1','w2',a['generation'],[])
        self.assertEqual(self.leader.task('T1')['status'],'DONE')

    def test_manual_acceptance_never_satisfied_by_worker_text_alone(self):
        _,h,j=self.prep()
        self.finish(h,j)
        a=self.leader.assign('w2')
        ev=self.leader.record_evidence('T1','w2',a['generation'],'test_report','it passed')
        with connect(self.leader.path) as db,write_tx(db):
            db.execute('UPDATE tasks SET acceptance_json=? WHERE id=?',
                       (dumps({'type':'evidence_only','required_evidence_kinds':['test_report']}),'T1'))
        with self.assertRaises(ContractError) as e:
            self.leader.complete('T1','w2',a['generation'],[ev['evidence_id']])
        self.assertEqual(e.exception.code,'HUMAN_ACCEPTANCE_REQUIRED')
        self.leader.human_complete('T1','human',[ev['evidence_id']])
        self.assertEqual(self.leader.task('T1')['status'],'DONE')

    def test_blob_hash_detects_corruption(self):
        a=self.leader.assign('w1')
        ev=self.leader.record_evidence('T1','w1',a['generation'],'diagnostic','out of memory')
        self.assertEqual(self.leader.read_evidence(ev['evidence_id'])['text'],'out of memory')
        path=self.leader.evidence_root/ev['sha256'][:2]/ev['sha256']
        path.write_text('tampered')
        with self.assertRaises(ContractError) as e:self.leader.read_evidence(ev['evidence_id'])
        self.assertEqual(e.exception.code,'EVIDENCE_CORRUPT')

    def test_corrupted_evidence_cannot_be_used_for_acceptance_or_lesson(self):
        a=self.leader.assign('w1')
        ev=self.leader.record_evidence('T1','w1',a['generation'],'diagnostic','verified observation')
        blob=self.leader.evidence_root/ev['sha256'][:2]/ev['sha256']
        blob.write_text('changed after indexing')
        with self.assertRaises(ContractError) as e:
            self.leader.propose_lesson('T1','w1',a['generation'],'lesson','hypothesis',
                                       {'gpu':'GTX1060'},[ev['evidence_id']])
        self.assertEqual(e.exception.code,'EVIDENCE_CORRUPT')
        with self.assertRaises(ContractError) as e:
            self.leader.complete('T1','w1',a['generation'],[ev['evidence_id']])
        self.assertEqual(e.exception.code,'EVIDENCE_CORRUPT')

    def test_worker_process_inherits_only_explicit_environment_allowlist(self):
        from bayesdesk.worker_pool import WorkerPool
        base=Path(self.tmp.name)
        output=base/'child-env.json'
        cmd=[sys.executable,'-c',
             'import os,json,pathlib;pathlib.Path('+repr(str(output))+').write_text(json.dumps(dict(os.environ)))']
        config={'max_concurrent':1,'env_allowlist':['MODEL_PROVIDER_TEST_KEY'],
                'workers':[{'worker_id':'worker-env','argv':cmd,'cwd':str(base)}]}
        with patch.dict(os.environ,{'BAYESDESK_SHARED_SECRET':'secret-do-not-send',
                                     'BAYESDESK_OPERATOR_TOKEN':'operator-do-not-send',
                                     'MY_UNAPPROVED_TOKEN':'private-do-not-send',
                                     'MODEL_PROVIDER_TEST_KEY':'approved-demo-key'}):
            pool=WorkerPool(self.leader,config,base/'capsules')
            pool.tick()
            for _ in range(100):
                if output.exists(): break
                time.sleep(.02)
        self.assertTrue(output.exists())
        child=json.loads(output.read_text())
        self.assertNotIn('BAYESDESK_SHARED_SECRET',child)
        self.assertNotIn('BAYESDESK_OPERATOR_TOKEN',child)
        self.assertNotIn('MY_UNAPPROVED_TOKEN',child)
        self.assertEqual(child['MODEL_PROVIDER_TEST_KEY'],'approved-demo-key')

    def test_broker_does_not_launch_a_job_after_recipe_drift(self):
        _,handoff,job=self.prep()
        accepted=self.broker.submit(job)
        self.leader.acknowledge_handoff(handoff['handoff_id'],accepted['receipt'])
        self.broker.recipes['smoke.v1']['argv']=['/bin/false']
        dispatch=self.broker.dispatch()
        self.assertEqual(dispatch[0]['state'],'UNKNOWN')
        self.assertEqual(self.broker.status(accepted['job_id'])['state'],'UNKNOWN')
        self.assertIn('RECIPE_VERSION_CHANGED',self.broker.status(accepted['job_id'])['last_error'])

    def test_early_result_file_never_releases_live_job_reservation(self):
        _,handoff,job=self.prep(inputs={'duration':2,'result':'pass'})
        job['timeout_seconds']=10
        accepted=self.broker.submit(job)
        self.leader.acknowledge_handoff(handoff['handoff_id'],accepted['receipt'])
        self.broker.dispatch()
        folder=self.broker._job_dir(accepted['job_id'])
        for _ in range(100):
            if (folder/'started.json').exists(): break
            time.sleep(.02)
        self.assertTrue((folder/'started.json').exists())
        (folder/'result.json').write_text(json.dumps({'job_id':accepted['job_id'],
                                                     'state':'SUCCEEDED','exit_code':0}))
        self.broker.reconcile()
        self.assertEqual(self.broker.status(accepted['job_id'])['state'],'RUNNING')
        self.assertEqual(self.broker.inspect()['reserved']['cpu_units'],2)
        for _ in range(100):
            self.broker.reconcile()
            if self.broker.status(accepted['job_id'])['state']=='SUCCEEDED':break
            time.sleep(.03)
        self.assertEqual(self.broker.status(accepted['job_id'])['state'],'SUCCEEDED')

    def test_verification_requires_distinct_worker_and_successful_job(self):
        a=self.leader.assign('w1')
        ev=self.leader.record_evidence('T1','w1',a['generation'],'diagnostic','failure in config C')
        lesson=self.leader.propose_lesson('T1','w1',a['generation'],'avoid config C','failure confirmed once',
                                          {'gpu':'GTX1060'},[ev['evidence_id']])
        self.assertEqual(self.leader.knowledge_search({'gpu':'GTX1060'}),[])
        self.leader.create_task('T2','D1','verify independently',[],[],ACCEPT,execution_policy=POLICY,
                                knowledge_scope={'gpu':'GTX1060'})
        _,h,j=self.prep(task='T2',worker='w2')
        self.finish(h,j)
        b=self.leader.assign('w2')
        proof=self.leader.record_evidence('T2','w2',b['generation'],'verification','repeated check')
        checked=self.leader.verify_lesson(lesson['lesson_id'],'T2','w2',b['generation'],[proof['evidence_id']])
        self.assertEqual(checked['state'],'PEER_CHECKED')
        self.assertEqual(self.leader.knowledge_search({'gpu':'GTX1060'}),[])
        self.leader.approve_lesson(lesson['lesson_id'],'human','verified independent repeat with same conditions')
        self.assertEqual(self.leader.knowledge_search({'gpu':'GTX1060'})[0]['state'],'VERIFIED')
        self.assertEqual(self.leader.knowledge_search({'gpu':'other'}),[])

    def test_exact_git_source_materialization_ignores_mutable_checkout(self):
        d=Path(self.tmp.name)/'repo'; d.mkdir()
        subprocess.run(['git','-C',str(d),'init','-q'],check=True)
        (d/'model.py').write_text('print(1)\n')
        subprocess.run(['git','-C',str(d),'add','model.py'],check=True)
        subprocess.run(['git','-C',str(d),'-c','user.email=x@y','-c','user.name=test','commit','-qm','initial'],check=True)
        sha=subprocess.check_output(['git','-C',str(d),'rev-parse','HEAD'],text=True).strip()
        (d/'model.py').write_text('print(2)\n')
        out=Path(self.tmp.name)/'job';out.mkdir()
        pinned=materialize_source({'source':{'repo_root':str(d),'workdir':'.'}},sha,out,str(d),require_pinned=True)
        self.assertEqual((Path(pinned)/'model.py').read_text(),'print(1)\n')
        with self.assertRaises(ContractError):
            materialize_source({},sha,Path(self.tmp.name)/'another',str(d),require_pinned=True)

if __name__=='__main__':unittest.main()
