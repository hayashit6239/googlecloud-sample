#!/usr/bin/env python3
"""
特徴量統計情報を取得するスクリプト

このスクリプトは BigQuery の ML.TFDV_VALIDATE 関数を使用して
特徴量の統計情報とドリフト検出を行います。

Requires: Python 3.11+
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery

# 共通ユーティリティからインポート
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.config import load_config


def get_basic_stats(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> pd.DataFrame:
    """基本的な統計情報を取得する"""
    query = f"""
    SELECT
        COUNT(*) as total_records,
        AVG(age) as avg_age,
        STDDEV(age) as std_age,
        MIN(age) as min_age,
        MAX(age) as max_age,
        AVG(income) as avg_income,
        STDDEV(income) as std_income,
        MIN(income) as min_income,
        MAX(income) as max_income
    FROM `{project_id}.{dataset_id}.{table_id}`
    """

    return client.query(query).to_dataframe()


def get_category_distribution(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> pd.DataFrame:
    """カテゴリの分布を取得する"""
    query = f"""
    SELECT
        category,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM `{project_id}.{dataset_id}.{table_id}`
    GROUP BY category
    ORDER BY count DESC
    """

    return client.query(query).to_dataframe()


def get_time_series_stats(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> pd.DataFrame:
    """時系列での統計情報を取得する"""
    query = f"""
    SELECT
        DATE(feature_timestamp) as date,
        COUNT(*) as record_count,
        AVG(age) as avg_age,
        AVG(income) as avg_income
    FROM `{project_id}.{dataset_id}.{table_id}`
    GROUP BY date
    ORDER BY date DESC
    LIMIT 10
    """

    return client.query(query).to_dataframe()


def detect_drift_with_tfdv(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    baseline_query: str,
    current_query: str,
) -> pd.DataFrame | None:
    """
    ML.TFDV_VALIDATE を使用してドリフトを検出する

    注意: この機能は BigQuery ML が有効なプロジェクトでのみ動作します
    """
    # TFDV を使用したドリフト検出クエリ
    query = f"""
    SELECT
        feature_name,
        anomaly_short_description,
        anomaly_long_description
    FROM ML.TFDV_VALIDATE(
        (SELECT * FROM ({baseline_query})),
        (SELECT * FROM ({current_query})),
        STRUCT(0.3 AS drift_threshold)
    )
    """

    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        print(f"⚠️  TFDV 検証でエラーが発生しました: {e}")
        return None


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description="特徴量統計情報を取得する"
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="詳細な統計情報を表示",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("特徴量統計情報")
    print("=" * 60)

    # 設定を読み込む
    config = load_config()
    project_id = config["project_id"]
    dataset_id = config["bigquery"]["dataset_id"]
    table_id = config["bigquery"]["table_id"]

    if project_id == "your-project-id":
        print("❌ エラー: vertexai-mlops/config.yaml の project_id を設定してください")
        sys.exit(1)

    print(f"\nプロジェクト: {project_id}")
    print(f"データセット: {dataset_id}")
    print(f"テーブル: {table_id}")

    # BigQuery クライアントを作成
    client = bigquery.Client(project=project_id)

    # 基本統計情報を取得
    print("\n📊 基本統計情報を取得中...")
    basic_stats = get_basic_stats(client, project_id, dataset_id, table_id)

    print("\n" + "=" * 40)
    print("数値特徴量の統計")
    print("=" * 40)
    print(f"総レコード数: {int(basic_stats['total_records'].iloc[0])}")
    print(f"\n【年齢 (age)】")
    print(f"  平均: {basic_stats['avg_age'].iloc[0]:.1f}")
    print(f"  標準偏差: {basic_stats['std_age'].iloc[0]:.1f}")
    print(f"  最小: {int(basic_stats['min_age'].iloc[0])}")
    print(f"  最大: {int(basic_stats['max_age'].iloc[0])}")

    print(f"\n【収入 (income)】")
    print(f"  平均: {basic_stats['avg_income'].iloc[0]:,.0f}")
    print(f"  標準偏差: {basic_stats['std_income'].iloc[0]:,.0f}")
    print(f"  最小: {basic_stats['min_income'].iloc[0]:,.0f}")
    print(f"  最大: {basic_stats['max_income'].iloc[0]:,.0f}")

    # カテゴリ分布を取得
    print("\n" + "=" * 40)
    print("カテゴリ分布")
    print("=" * 40)
    category_dist = get_category_distribution(client, project_id, dataset_id, table_id)
    for _, row in category_dist.iterrows():
        bar = "█" * int(row["percentage"] / 5)
        print(f"  {row['category']}: {row['count']} ({row['percentage']}%) {bar}")

    if args.detailed:
        # 時系列統計を取得
        print("\n" + "=" * 40)
        print("時系列統計（直近10日）")
        print("=" * 40)
        time_stats = get_time_series_stats(client, project_id, dataset_id, table_id)
        if not time_stats.empty:
            for _, row in time_stats.iterrows():
                print(
                    f"  {row['date']}: "
                    f"件数={int(row['record_count'])}, "
                    f"平均年齢={row['avg_age']:.1f}, "
                    f"平均収入={row['avg_income']:,.0f}"
                )
        else:
            print("  データがありません")

    print("\n" + "=" * 60)
    print("✅ 処理が完了しました")
    print("=" * 60)
    print("\n💡 ヒント:")
    print("  - ドリフトを検出するには: python simulation/simulate_drift.py を実行")
    print("  - モニタリングジョブ: python monitoring/run_monitor_job.py を実行")


if __name__ == "__main__":
    main()
