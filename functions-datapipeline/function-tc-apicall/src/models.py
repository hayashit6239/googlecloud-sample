from dataclasses import dataclass
from typing import Dict, Optional, Any


@dataclass
class MeasurementPoint:
    """計測データポイント"""

    min_value: Optional[float]
    max_value: Optional[float]

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> Optional["MeasurementPoint"]:
        """辞書データからオブジェクトを作成"""
        if data is None:
            return None
        return cls(min_value=data.get("min"), max_value=data.get("max"))


@dataclass
class SensorSchema:
    """センサースキーマ情報"""

    name: str
    unit: str
    type: str