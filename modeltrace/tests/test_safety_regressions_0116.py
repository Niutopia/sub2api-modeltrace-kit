import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch
import pytest
from modeltrace.config import ConfigError,load_config
from modeltrace.service import utc_iso
from modeltrace.transport import ProbeTransport
from tests.test_per_account_d2 import MockHostClient,MockProbeTransport,make_service

MODEL='gpt-6-astra'

def make(tmp_path, clock, model=MODEL, overrides=None, paused=False):
    account={'account_id':38,'name':'mock','type':'oauth','platform':'openai','schedulable':True,'models':[model]}
    if paused:
        account['paused_models']=[{'model':model,'until':utc_iso(clock()+3600)}]
    host=MockHostClient([account]); transport=MockProbeTransport()
    with patch('modeltrace.service.HostClient',return_value=host),patch('socket.socket.connect',side_effect=AssertionError('network disabled')):
        svc=make_service(tmp_path,clock,host_client=host,transport=transport,config_overrides=overrides)
    svc._refresh_accounts(clock())
    return svc,host,transport

def calibration(model=MODEL,effort='low',account=38):
    return {'account_id':account,'model':model,'reasoning_effort':effort,'reference':'synthetic-route-calibration'}

def action(svc,clock,status='suspect',model=MODEL):
    target,members=svc._get_target_and_members(38)
    svc._handle_auto_pause_resume(target=target,members=members,model=model,status=status,message_code='compatible' if status=='match' else 'repeated_other_model',best_model=MODEL if status=='match' else 'gpt-5.6-sol',now=clock())

@pytest.mark.parametrize('model,effort',[('gpt-5.6-sol','none'),(MODEL,'low'),(MODEL,'medium')])
def test_uncalibrated_never_pauses(tmp_path,fake_clock,model,effort):
    svc,host,_=make(tmp_path,fake_clock,model=model,overrides={'reasoning_effort_overrides':{model:effort}})
    action(svc,fake_clock,model=model)
    assert host.pause_calls==[]
    assert not svc.account_snapshot(38)['per_model'][0]['auto_actions_eligible']

@pytest.mark.parametrize('record',[calibration(effort='none'),calibration(account=39),calibration(model='gpt-5.6-sol')])
def test_calibration_must_match_account_route_effort(tmp_path,fake_clock,record):
    svc,host,_=make(tmp_path,fake_clock,overrides={'auto_pause_calibrations':[record]})
    action(svc,fake_clock)
    assert host.pause_calls==[]

def test_explicit_calibration_allows_pause(tmp_path,fake_clock):
    svc,host,_=make(tmp_path,fake_clock,overrides={'auto_pause_calibrations':[calibration()]})
    action(svc,fake_clock)
    assert host.pause_calls[0]['minutes']==1440

@pytest.mark.parametrize('raises',[False,True])
def test_failed_resume_preserves_pause(tmp_path,fake_clock,raises):
    svc,host,_=make(tmp_path,fake_clock,overrides={'auto_pause_calibrations':[calibration()]},paused=True)
    def fail(*args):
        if raises: raise RuntimeError('synthetic failure')
        return False
    host.resume_model=fail
    action(svc,fake_clock,'match')
    assert json.loads(svc.db.get_account(38)['paused_models_json'])

def test_confirmed_resume_clears_pause(tmp_path,fake_clock):
    svc,host,_=make(tmp_path,fake_clock,overrides={'auto_pause_calibrations':[calibration()]},paused=True)
    action(svc,fake_clock,'match')
    assert json.loads(svc.db.get_account(38)['paused_models_json'])==[]

def test_failed_manual_reset_is_not_success(tmp_path,fake_clock):
    svc,host,_=make(tmp_path,fake_clock,paused=True);host.resume_model=lambda *a:False
    assert svc.reset_account_model(38,model=MODEL)=={'reset':False,'resumed':0,'failed_account_ids':[38]}
    assert json.loads(svc.db.get_account(38)['paused_models_json'])
    assert svc.db.get_account_latest_round(38) is None

def test_disabled_does_not_schedule_or_auto_pause(tmp_path,fake_clock):
    svc,host,_=make(tmp_path,fake_clock,overrides={'enabled':False,'auto_pause_calibrations':[calibration()]})
    svc.db.set_account_next_run(38,fake_clock(),now=fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert svc.db.claim_next_account(now=fake_clock()) is None
    action(svc,fake_clock);assert host.pause_calls==[]
    svc.enqueue_manual_account(38,model=MODEL)
    assert svc.db.claim_next_account(now=fake_clock(),allow_scheduled=False).trigger=='manual'

def test_disabled_queue_claim_leaves_scheduled_job_untouched(tmp_path,fake_clock):
    svc,_,_=make(tmp_path,fake_clock)
    svc.db.enqueue_account(38,MODEL,now=fake_clock(),available_at=fake_clock(),trigger='scheduled',retest_index=0)
    assert svc.db.claim_next_account(now=fake_clock(),allow_scheduled=False) is None
    assert svc.db.claim_next_account(now=fake_clock(),allow_scheduled=True).trigger=='scheduled'

def test_configured_route_alias_uses_canonical_calibration(tmp_path,fake_clock,monkeypatch):
    alias=MODEL+'-basispoints'
    svc,host,transport=make(tmp_path,fake_clock,model=alias,overrides={'model_aliases':{alias:MODEL}})
    assert alias in host.fetch_calls[-1]
    assert json.loads(svc.db.get_account(38)['models_json'])==[alias]
    monkeypatch.setattr('modeltrace.service.analyze_outputs',lambda *a,**k:{'prediction':MODEL,'results':[{'model':MODEL,'probability':0.95}]})
    svc.enqueue_manual_account(38,model=alias)
    svc._run_account_job(svc.db.claim_next_account(now=fake_clock()))
    result=svc.db.get_account_latest_round(38)
    assert result['model']==alias and result['status']=='match'
    assert result['reasoning_effort']=='low'
    assert host.pause_calls==[]

def test_transport_accepts_only_declared_alias_and_preserves_request_name():
    alias=MODEL+'-basispoints';t=ProbeTransport(endpoint='https://mock.invalid',api_key='mock',model_aliases={alias:MODEL})
    payload=t._payload(alias,{'prompt':'text-only'})
    assert payload['model']==alias and payload['reasoning']['effort']=='low'
    obs=SimpleNamespace(expected_model=alias,upstream_model=None)
    assert t._observe_model({'model':MODEL},obs)
    assert not t._observe_model({'model':'gpt-5.6-sol'},obs)
    obs=SimpleNamespace(expected_model=MODEL,upstream_model=None)
    assert not t._observe_model({'model':alias},obs)

@pytest.mark.parametrize('value',[{},[{}],[{'account_id':True,'model':MODEL,'reasoning_effort':'low','reference':'x'}],[calibration(),calibration()]])
def test_invalid_calibration_config_rejected(tmp_path,fake_clock,value):
    with pytest.raises(ConfigError): make(tmp_path,fake_clock,overrides={'auto_pause_calibrations':value})

@pytest.mark.parametrize('aliases',[{'x':'y','y':'x'},{MODEL:MODEL},{'x':'no-such-bank-model'}])
def test_bad_alias_config_rejected(tmp_path,fake_clock,aliases):
    with pytest.raises(ConfigError): make(tmp_path,fake_clock,overrides={'model_aliases':aliases})

def test_uncalibrated_cluster_member_blocks_automatic_actions(tmp_path,fake_clock):
    svc,host,_=make(tmp_path,fake_clock,overrides={'auto_pause_calibrations':[calibration()]})
    host.accounts_data[0]['cluster_id']='mock-cluster'
    host.accounts_data.append(dict(host.accounts_data[0],account_id=39))
    svc._refresh_accounts(fake_clock())
    action(svc,fake_clock)
    assert host.pause_calls==[]

def test_partial_manual_reset_preserves_failed_member(tmp_path,fake_clock):
    svc,host,_=make(tmp_path,fake_clock,paused=True)
    host.accounts_data[0]['cluster_id']='mock-cluster'
    host.accounts_data.append(dict(host.accounts_data[0],account_id=39))
    svc._refresh_accounts(fake_clock())
    host.resume_model=lambda account,model: account==38
    result=svc.reset_account_model(38,model=MODEL)
    assert result=={'reset':False,'resumed':1,'failed_account_ids':[39]}
    assert json.loads(svc.db.get_account(38)['paused_models_json'])==[]
    assert json.loads(svc.db.get_account(39)['paused_models_json'])

def test_disabled_legacy_queue_does_not_claim_scheduled(tmp_path,fake_clock):
    svc,_,_=make(tmp_path,fake_clock)
    svc.db.enqueue(1,now=fake_clock(),available_at=fake_clock(),trigger='scheduled',retest_index=0)
    assert svc.db.claim_next(now=fake_clock(),allow_scheduled=False) is None
    assert svc.db.claim_next(now=fake_clock()).trigger=='scheduled'
