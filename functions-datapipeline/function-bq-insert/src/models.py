from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict


@dataclass
class InferenceResult:
    """推論結果データモデル"""
    
    timestamp: datetime
    inference_value: float
    
    def to_dict(self) -> Dict[str, Any]:
        """BigQuery挿入用の辞書に変換"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "inference_value": self.inference_value
        }