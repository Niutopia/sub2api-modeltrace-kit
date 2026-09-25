from dataclasses import replace
from modeltrace.config import MonitorConfig
from tests.conftest import build_service


def test_hourly_four_models_full_hour_stagger(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    models = ['gpt-5.4', 'gpt-5.5', 'gpt-5.6-sol', 'gpt-5.6-terra']
    price = service.config.price_for('gpt-5.4')
    service.config = replace(service.config, interval_seconds=3600, enabled=True,
                            monitors={i+1: MonitorConfig(i+1,m,True,True) for i,m in enumerate(models)},
                            pricing_upper_bound={m:price for m in models})
    anchor = fake_clock()
    assert [service._next_scheduled_run(i,anchor)-anchor for i in range(1,5)] == [0,900,1800,2700]
    assert [service._next_scheduled_run(i,anchor+3600)-anchor for i in range(1,5)] == [3600,4500,5400,6300]
    service.db.close()


def test_hourly_stale_after_two_intervals(tmp_path, fake_clock):
    from tests.conftest import run_one
    service, _, _ = build_service(tmp_path, fake_clock)
    service.config = replace(service.config, interval_seconds=3600)
    run_one(service)
    fake_clock.advance(3601)
    assert service.snapshot(1)['stale'] is False
    fake_clock.advance(3600)
    assert service.snapshot(1)['stale'] is True
    service.db.close()
