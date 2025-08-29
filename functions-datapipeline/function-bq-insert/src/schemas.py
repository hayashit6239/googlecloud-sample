"""
NodeAI APIのリクエスト/レスポンスのデータ形式を定義
"""
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class AnomalyResult:
    """異常検知結果の単一エントリ"""
    timestamp: str
    reconstructionError: Optional[float]


@dataclass
class ThresholdValues:
    """閾値設定"""
    sigma2: float  # 2σ閾値
    sigma3: float  # 3σ閾値


@dataclass
class NodeAIApiResponse:
    """NodeAI APIからのレスポンス構造"""
    results: List[AnomalyResult]
    threshold: ThresholdValues