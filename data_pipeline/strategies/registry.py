"""Strategy registry used to resolve ETL implementations by layer."""

from __future__ import annotations

from dataclasses import dataclass

from data_pipeline.strategies.bronze.extract import BronzeAsyncAPIExtract
from data_pipeline.strategies.bronze.load import BronzeLoad
from data_pipeline.strategies.bronze.quality import BronzeQuality
from data_pipeline.strategies.bronze.transform import BronzeTransform
from data_pipeline.strategies.gold.extract import GoldExtract
from data_pipeline.strategies.gold.load import GoldLoad
from data_pipeline.strategies.gold.quality import GoldQuality
from data_pipeline.strategies.gold.transform import GoldTransform
from data_pipeline.strategies.interfaces import DataQualityStrategy, Strategy
from data_pipeline.strategies.silver.extract import SilverExtract
from data_pipeline.strategies.silver.load import SilverLoad
from data_pipeline.strategies.silver.quality import SilverQuality
from data_pipeline.strategies.silver.transform import SilverTransform


@dataclass(frozen=True)
class StrategyBundle:
    """Container for extract, transform, load, and quality strategies."""

    extract: Strategy
    transform: Strategy
    load: Strategy
    quality: DataQualityStrategy


class StrategyRegistry:
    """Resolve strategy bundles by pipeline layer name."""

    _registry: dict[str, StrategyBundle] = {
        "bronze": StrategyBundle(
            extract=BronzeAsyncAPIExtract(),
            transform=BronzeTransform(),
            load=BronzeLoad(),
            quality=BronzeQuality(),
        ),
        "silver": StrategyBundle(
            extract=SilverExtract(),
            transform=SilverTransform(),
            load=SilverLoad(),
            quality=SilverQuality(),
        ),
        "gold": StrategyBundle(
            extract=GoldExtract(),
            transform=GoldTransform(),
            load=GoldLoad(),
            quality=GoldQuality(),
        ),
    }

    @classmethod
    def get(cls, layer: str) -> StrategyBundle:
        """Return strategies configured for a given layer.

        Args:
            layer: Pipeline layer name (bronze, silver, or gold).

        Returns:
            Strategy bundle for the requested layer.

        Raises:
            ValueError: If the layer is not supported.
        """
        normalized_layer = layer.strip().lower()
        if normalized_layer not in cls._registry:
            supported = ", ".join(sorted(cls._registry.keys()))
            raise ValueError(
                f"Invalid layer '{layer}'. Supported layers: {supported}."
            )

        return cls._registry[normalized_layer]
