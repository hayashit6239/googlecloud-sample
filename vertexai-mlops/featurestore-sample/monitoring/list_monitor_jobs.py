#!/usr/bin/env python3
"""
FeatureMonitorJob の一覧を取得するスクリプト

このスクリプトは FeatureMonitor に対して実行された
モニタリングジョブの一覧を表示します。

Requires: Python 3.11+
"""

import argparse
import sys
from pathlib import Path
from typing import Any

from google.cloud import aiplatform
from vertexai.resources.preview import feature_store

# 共通ユーティリティからインポート
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.config import load_config


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


def list_feature_monitor_jobs(
    monitor: feature_store.FeatureMonitor,
) -> list[feature_store.FeatureMonitor.FeatureMonitorJob]:
    """
    FeatureMonitorJob の一覧を取得する

    Args:
        monitor: FeatureMonitor インスタンス

    Returns:
        FeatureMonitorJob のリスト
    """
    return list(monitor.list_feature_monitor_jobs())


def format_job_info(
    job: feature_store.FeatureMonitor.FeatureMonitorJob,
) -> str:
    """ジョブ情報をフォーマットする"""
    job_id = job.resource_name.split("/")[-1] if job.resource_name else "N/A"
    create_time = str(job.create_time) if job.create_time else "N/A"

    # ドリフト検出結果から状態を判断
    feature_stats = job.feature_stats_and_anomalies
    if feature_stats:
        # 結果がある = 完了
        has_drift = any(
            getattr(stat, 'drift_detected', False) for stat in feature_stats
        )
        icon = "🔴" if has_drift else "✅"
        status = "ドリフト検出" if has_drift else "正常"
    else:
        # 結果がない = 実行中または未完了
        icon = "⏳"
        status = "実行中/結果なし"

    return f"{icon} [{job_id}] {status} (作成: {create_time[:19]})"


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description="FeatureMonitorJob の一覧を取得する"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="取得するジョブ数（デフォルト: 10）",
    )
    parser.add_argument(
        "--job-id",
        type=str,
        help="特定のジョブの詳細を表示",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("FeatureMonitorJob 一覧")
    print("=" * 60)

    # 設定を読み込む
    config = load_config()
    project_id = config["project_id"]
    location = config["location"]
    feature_group_name = config["feature_store"]["feature_group_name"]
    monitor_name = config["feature_monitor"]["name"]

    if project_id == "your-project-id":
        print("❌ エラー: vertexai-mlops/config.yaml の project_id を設定してください")
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

    if args.job_id:
        # 特定のジョブの詳細を表示
        print(f"\n📋 ジョブ詳細を取得中: {args.job_id}")
        try:
            job = monitor.get_feature_monitor_job(args.job_id)

            print("\n" + "=" * 40)
            print(f"ジョブ ID: {args.job_id}")
            print("=" * 40)

            print(f"リソース名: {job.resource_name}")
            print(f"作成日時: {job.create_time}")

            feature_stats = job.feature_stats_and_anomalies
            if feature_stats:
                print("\n📊 特徴量統計とアノマリ:")
                for stat in feature_stats:
                    feature_id = stat.feature_id if hasattr(stat, 'feature_id') else "N/A"
                    drift_detected = stat.drift_detected if hasattr(stat, 'drift_detected') else False
                    drift_score = stat.drift_score if hasattr(stat, 'drift_score') else None
                    drift_icon = "🔴" if drift_detected else "🟢"
                    print(f"   {drift_icon} {feature_id}:")
                    print(f"      ドリフト検出: {drift_detected}")
                    if drift_score is not None:
                        print(f"      ドリフトスコア: {drift_score}")

                    # 統計情報
                    if hasattr(stat, 'feature_stats') and stat.feature_stats:
                        fs = stat.feature_stats
                        if hasattr(fs, 'numeric_stats') and fs.numeric_stats:
                            num_stats = fs.numeric_stats
                            print(f"      平均: {getattr(num_stats, 'mean', 'N/A')}")
                            print(f"      標準偏差: {getattr(num_stats, 'std_dev', 'N/A')}")
                        if hasattr(fs, 'string_stats') and fs.string_stats:
                            str_stats = fs.string_stats
                            if hasattr(str_stats, 'top_values'):
                                top_values = str_stats.top_values[:3]
                                print(f"      上位値: {[getattr(v, 'value', 'N/A') for v in top_values]}")
            else:
                print("\n   統計情報はありません")

        except Exception as e:
            print(f"❌ エラー: {e}")
            sys.exit(1)
    else:
        # ジョブ一覧を取得
        print(f"\n📋 ジョブ一覧を取得中（最大 {args.limit} 件）...")
        try:
            jobs = list_feature_monitor_jobs(monitor)
            jobs = jobs[:args.limit]  # 最大件数に制限

            if not jobs:
                print("\nℹ️  ジョブが見つかりませんでした")
            else:
                print(f"\n📊 {len(jobs)} 件のジョブが見つかりました:\n")
                for job in jobs:
                    print(f"   {format_job_info(job)}")

                print("\n💡 ヒント: 特定のジョブの詳細を見るには:")
                print("   python monitoring/list_monitor_jobs.py --job-id <JOB_ID>")

        except Exception as e:
            print(f"\n❌ ジョブ一覧の取得に失敗しました: {e}")
            sys.exit(1)

    print("\n" + "=" * 60)
    print("✅ 処理が完了しました")
    print("=" * 60)


if __name__ == "__main__":
    main()
