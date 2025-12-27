"""
設定ファイル読み込みユーティリティ

vertexai-mlops/config.yaml から featurestore セクションを読み込み、
従来の設定構造と互換性のある形式で返します。
"""

from pathlib import Path
from typing import Any

import yaml


def load_config() -> dict[str, Any]:
    """
    設定ファイルを読み込む（vertexai-mlops/config.yaml から featurestore セクションを取得）

    Returns:
        従来の設定構造と互換性のある辞書
    """
    # vertexai-mlops/config.yaml へのパス
    config_path = Path(__file__).parent.parent.parent / "config.yaml"

    with open(config_path, "r", encoding="utf-8") as f:
        root_config = yaml.safe_load(f)

    # featurestore セクションを取得
    fs_config = root_config.get("featurestore", {})

    # 従来の構造と互換性のある形式に変換
    return {
        "project_id": root_config["project_id"],
        "location": root_config["location"],
        "bigquery": fs_config.get("bigquery", {}),
        "feature_store": {
            "feature_group_name": fs_config.get("feature_group_name", ""),
            "features": fs_config.get("features", []),
        },
        "feature_monitor": fs_config.get("feature_monitor", {}),
        "sample_data": fs_config.get("sample_data", {}),
    }
