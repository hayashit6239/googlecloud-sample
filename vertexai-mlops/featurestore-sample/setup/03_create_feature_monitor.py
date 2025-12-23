#!/usr/bin/env python3
"""
FeatureMonitor を作成するスクリプト

このスクリプトは以下を実行します:
1. FeatureGroup に対する FeatureMonitor の作成
2. 各特徴量のドリフト閾値の設定

Requires: Python 3.11+
"""

import sys
from pathlib import Path
from typing import Any

import yaml
from google.cloud import aiplatform
from vertexai.resources.preview import feature_store


def load_config() -> dict[str, Any]:
    """設定ファイルを読み込む"""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_feature_group(
    feature_group_name: str,
) -> feature_store.FeatureGroup | None:
    """既存の FeatureGroup を取得する"""
    try:
        return feature_store.FeatureGroup(feature_group_name)
    except Exception:
        return None


def get_feature_monitor(
    feature_group: feature_store.FeatureGroup,
    monitor_name: str,
) -> feature_store.FeatureMonitor | None:
    """既存の FeatureMonitor を取得する"""
    try:
        return feature_group.get_feature_monitor(monitor_name)
    except Exception:
        return None


def create_feature_monitor(
    feature_group: feature_store.FeatureGroup,
    monitor_name: str,
    features_config: list[dict[str, Any]],
    cron_schedule: str,
) -> feature_store.FeatureMonitor:
    """
    FeatureMonitor を作成する

    Args:
        feature_group: 親となる FeatureGroup
        monitor_name: FeatureMonitor の名前
        features_config: 特徴量設定のリスト
        cron_schedule: cron スケジュール

    Returns:
        作成された FeatureMonitor
    """
    # feature_selection_configs は List[Tuple[str, float]] 形式
    # (feature_name, drift_threshold) のタプルリスト
    feature_selection_configs = [
        (feature["name"], feature["drift_threshold"])
        for feature in features_config
    ]

    monitor = feature_group.create_feature_monitor(
        name=monitor_name,
        schedule_config=cron_schedule,
        feature_selection_configs=feature_selection_configs,
    )
    return monitor


def main():
    """メイン処理"""
    print("=" * 60)
    print("FeatureMonitor セットアップ")
    print("=" * 60)

    # 設定を読み込む
    config = load_config()
    project_id = config["project_id"]
    location = config["location"]
    feature_group_name = config["feature_store"]["feature_group_name"]
    features_config = config["feature_store"]["features"]
    monitor_name = config["feature_monitor"]["name"]
    cron_schedule = config["feature_monitor"]["cron_schedule"]

    if project_id == "your-project-id":
        print("❌ エラー: config/config.yaml の project_id を設定してください")
        sys.exit(1)

    print(f"\nプロジェクト: {project_id}")
    print(f"リージョン: {location}")
    print(f"FeatureGroup: {feature_group_name}")
    print(f"FeatureMonitor: {monitor_name}")
    print(f"スケジュール: {cron_schedule}")

    # Vertex AI を初期化
    print("\n🔑 Vertex AI を初期化中...")
    aiplatform.init(project=project_id, location=location)
    print("✅ 初期化しました")

    # FeatureGroup を取得
    print("\n🔍 FeatureGroup を取得中...")
    feature_group = get_feature_group(feature_group_name)

    if feature_group is None:
        print(f"❌ FeatureGroup '{feature_group_name}' が見つかりません")
        print("   先に python setup/02_create_feature_group.py を実行してください")
        sys.exit(1)

    print(f"✅ FeatureGroup '{feature_group_name}' を取得しました")

    # FeatureMonitor が既に存在するか確認
    print("\n🔍 既存の FeatureMonitor を確認中...")
    existing_monitor = get_feature_monitor(feature_group, monitor_name)

    if existing_monitor is not None:
        print(f"ℹ️  FeatureMonitor '{monitor_name}' は既に存在します")
        print(f"\n📋 FeatureMonitor 情報:")
        print(f"   リソース名: {existing_monitor.resource_name}")
        if existing_monitor.schedule_config:
            print(f"   スケジュール: {existing_monitor.schedule_config}")
    else:
        # FeatureMonitor を作成
        print("\n📡 FeatureMonitor を作成中...")
        print("   モニタリング対象の特徴量:")
        for feature in features_config:
            print(f"   - {feature['name']} (閾値: {feature['drift_threshold']})")

        try:
            monitor = create_feature_monitor(
                feature_group=feature_group,
                monitor_name=monitor_name,
                features_config=features_config,
                cron_schedule=cron_schedule,
            )
            print(f"\n✅ FeatureMonitor '{monitor_name}' を作成しました")
            print(f"   リソース名: {monitor.resource_name}")
        except Exception as e:
            print(f"\n❌ FeatureMonitor の作成に失敗しました: {e}")
            sys.exit(1)

    print("\n" + "=" * 60)
    print("✅ FeatureMonitor セットアップが完了しました")
    print("=" * 60)
    print("\n次のステップ:")
    print("  - モニタリングジョブを実行: python monitoring/run_monitor_job.py")
    print("  - ドリフトをシミュレート: python simulation/simulate_drift.py")


if __name__ == "__main__":
    main()
