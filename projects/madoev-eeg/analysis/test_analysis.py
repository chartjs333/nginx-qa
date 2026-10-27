"""Offline tests: python -m pytest -q test_analysis.py."""
import numpy as np
import pytest
from pathlib import Path
from analyze_eeg_ar import fit_order_bic, rolling_estimates, assert_atlas_approved
from compare_sessions import compare

def ar2_sequence(n=8000, seed=42):
    rng = np.random.default_rng(seed)
    x = np.zeros(n)
    noise = rng.normal(size=n)
    for i in range(2, n):
        x[i] = 0.72*x[i-1] - 0.18*x[i-2] + noise[i]
    return x

def test_finds_low_order_ar_process():
    est = fit_order_bic(ar2_sequence(), max_order=15)
    assert 1 <= est['order'] <= 4
    assert len(est['candidate_orders']) == 15

def test_windowing_rejects_artifact():
    x = ar2_sequence(n=4000)*10
    x[200:205] = 800
    res = rolling_estimates(x, fs=100, window_sec=10, max_order=10, ptp_reject_uv=250)
    assert res['n_rejected'] >= 1
    assert res['n_accepted'] >= 1

def test_all_bad_windows_have_null_summary():
    x = np.zeros(2000)
    res = rolling_estimates(x, fs=100, window_sec=10, max_order=5)
    assert res['median_order'] is None
    assert res['n_rejected'] >= 1

def test_historical_atlas_is_blocked():
    atlas = Path(__file__).resolve().parent.parent/'atlas'/'atlas_zones.json'
    with pytest.raises(RuntimeError):
        assert_atlas_approved(atlas)

def test_invalid_model_range_is_blocked():
    with pytest.raises(ValueError):
        fit_order_bic(ar2_sequence(), max_order=0)

def test_comparison_relative_to_repeated_baseline():
    def make(value):
        return {'channels': {'C3': {'median_order': value, 'n_accepted': 7}}}
    obj = compare({'B1': make(8.), 'B2': make(10.), 'P1': make(12.)})
    assert obj['channels']['C3']['reference_median_order'] == 9.
    assert obj['channels']['C3']['baseline_absolute_test_retest_difference'] == 2.
    assert obj['channels']['C3']['sessions']['P1']['delta_from_two_baselines'] == 3.

def test_two_baselines_required():
    with pytest.raises(ValueError):
        compare({'B1': {'channels': {}}})
