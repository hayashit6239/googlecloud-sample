"""
Vertex AI Pipeline 定義

このモジュールでは、ML パイプラインを定義しています。
"""

from pipelines.training_pipeline import ml_training_pipeline

__all__ = [
    "ml_training_pipeline",
]
