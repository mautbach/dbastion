"""Test cost threshold checking."""


from dbastion.adapters._base import CostEstimate, CostUnit
from dbastion.adapters.cost import check_cost_threshold
from dbastion.diagnostics import codes


def test_under_gb_threshold():
    estimate = CostEstimate(estimated_gb=5.0, unit=CostUnit.BYTES)
    assert check_cost_threshold(estimate, max_gb=10.0) is None


def test_over_gb_threshold():
    estimate = CostEstimate(estimated_gb=15.0, estimated_cost_usd=0.09, unit=CostUnit.BYTES)
    diag = check_cost_threshold(estimate, max_gb=10.0)
    assert diag is not None
    assert diag.code == codes.COST_OVER_THRESHOLD
    assert "15.0 GB" in diag.message
    assert "10.0 GB" in diag.message


def test_under_usd_threshold():
    estimate = CostEstimate(estimated_cost_usd=0.05, unit=CostUnit.BYTES)
    assert check_cost_threshold(estimate, max_usd=1.0) is None


def test_over_usd_threshold():
    estimate = CostEstimate(estimated_cost_usd=5.50, unit=CostUnit.BYTES)
    diag = check_cost_threshold(estimate, max_usd=1.0)
    assert diag is not None
    assert diag.code == codes.COST_OVER_THRESHOLD
    assert "$5.50" in diag.message


def test_no_thresholds_returns_none():
    estimate = CostEstimate(estimated_gb=100.0, estimated_cost_usd=50.0)
    assert check_cost_threshold(estimate) is None


def test_gb_threshold_with_no_gb_estimate():
    """If estimate has no GB info, GB threshold can't trigger."""
    estimate = CostEstimate(raw_value=100.0, unit=CostUnit.COST_UNITS)
    assert check_cost_threshold(estimate, max_gb=10.0) is None


def test_gb_checked_before_usd():
    """GB threshold is checked first; if it triggers, USD is not checked."""
    estimate = CostEstimate(estimated_gb=20.0, estimated_cost_usd=0.01, unit=CostUnit.BYTES)
    diag = check_cost_threshold(estimate, max_gb=10.0, max_usd=100.0)
    assert diag is not None
    assert "GB" in diag.message
