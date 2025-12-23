#!/usr/bin/env python3
"""
FeatureMonitorJob を手動実行するスクリプト

このスクリプトは FeatureMonitor に対してモニタリングジョブを
手動で実行（オンデマンド実行）します。

Requires: Python 3.11+
"""

import argparse
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


def run_feature_monitor_job(
    monitor: feature_store.FeatureMonitor,
) -> feature_store.FeatureMonitor.FeatureMonitorJob:
    """
    FeatureMonitorJob を実行する

    Args:
        monitor: FeatureMonitor インスタンス

    Returns:
        作成された FeatureMonitorJob
    """
    job = monitor.create_feature_monitor_job()
    return job


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description="FeatureMonitorJob を手動実行する"
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="ジョブの完了を待機する",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="待機時のタイムアウト（秒、デフォルト: 600）",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("FeatureMonitorJob 実行")
    print("=" * 60)

    # 設定を読み込む
    config = load_config()
    project_id = config["project_id"]
    location = config["location"]
    feature_group_name = config["feature_store"]["feature_group_name"]
    monitor_name = config["feature_monitor"]["name"]

    if project_id == "your-project-id":
        print("❌ エラー: config/config.yaml の project_id を設定してください")
        sys.exit(1)

    print(f"\nプロジェクト: {project_id}")
    print(f"リージョン: {location}")
    print(f"FeatureGroup: {feature_group_name}")
    print(f"FeatureMonitor: {monitor_name}")

    # Vertex AI を初期化
    print("\n🔑 Vertex AI を初期化中...")
    aiplatform.init(project=project_id, location=location)
    print("✅ 初期化しました")

    # FeatureGroup を取得
    print("\n🔍 FeatureGroup を取得中...")
    feature_group = get_feature_group(feature_group_name)

    if feature_group is None:
        print(f"❌ FeatureGroup '{feature_group_name}' が見つかりません")
        sys.exit(1)

    # FeatureMonitor を取得
    print("🔍 FeatureMonitor を取得中...")
    monitor = get_feature_monitor(feature_group, monitor_name)

    if monitor is None:
        print(f"❌ FeatureMonitor '{monitor_name}' が見つかりません")
        sys.exit(1)

    # ジョブを実行
    print("\n🚀 FeatureMonitorJob を実行中...")
    try:
        job = run_feature_monitor_job(monitor)

        job_id = job.resource_name.split("/")[-1] if job.resource_name else "N/A"

        print(f"\n✅ FeatureMonitorJob を開始しました")
        print(f"   リソース名: {job.resource_name}")
        print(f"   ジョブID: {job_id}")

        if args.wait:
            print(f"\n⏳ ジョブの完了を待機中 (タイムアウト: {args.timeout}秒)...")

            # SDK の wait メソッドを使用
            job.wait()

            print(f"\n📊 ジョブが完了しました")

            # ドリフト検出結果を表示
            feature_stats = job.feature_stats_and_anomalies
            if feature_stats:
                print("\n📈 特徴量統計とアノマリ:")
                for stat in feature_stats:
                    feature_id = stat.feature_id if hasattr(stat, 'feature_id') else "N/A"
                    drift_detected = stat.drift_detected if hasattr(stat, 'drift_detected') else False
                    drift_icon = "🔴" if drift_detected else "🟢"
                    print(f"   {drift_icon} {feature_id}: ドリフト検出 = {drift_detected}")
            else:
                print("\n   ドリフト検出結果はありません")

    except Exception as e:
        print(f"\n❌ ジョブの実行に失敗しました: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✅ 処理が完了しました")
    print("=" * 60)


if __name__ == "__main__":
    main()
