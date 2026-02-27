"""Unit tests for strategy registry resolution."""

import pytest

from data_pipeline.strategies import (
    SilverExtract,
    SilverLoad,
    SilverQuality,
    SilverTransform,
    StrategyRegistry,
)


def test_registry_returns_bundle_for_layer():
    """Ensure known layer returns the expected strategy implementations."""
    bundle = StrategyRegistry.get("silver")
    assert isinstance(bundle.extract, SilverExtract)
    assert isinstance(bundle.transform, SilverTransform)
    assert isinstance(bundle.load, SilverLoad)
    assert isinstance(bundle.quality, SilverQuality)


def test_registry_rejects_invalid_layer():
    """Ensure unsupported layers raise `ValueError`."""
    with pytest.raises(ValueError):
        StrategyRegistry.get("platinum")
