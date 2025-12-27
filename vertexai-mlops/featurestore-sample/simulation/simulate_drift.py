#!/usr/bin/env python3
"""
特徴量ドリフトをシミュレートするスクリプト

このスクリプトは以下を実行します:
1. ドリフトしたデータの生成（設定ファイルの drifted 設定を使用）
2. BigQuery テーブルへのデータ追加
3. ドリフト前後のデータ比較

Requires: Python 3.11+
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from google.cloud import bigquery

# 共通ユーティリティからインポート
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.config import load_config


def generate_drifted_data(config: dict[str, Any]) -> pd.DataFrame:
    """
    ドリフトしたサンプル特徴データを生成する

    Args:
        config: 設定ファイルの内容

    Returns:
        生成されたドリフトデータの DataFrame
    """
    num_records = config["sample_data"]["num_records"]
    data_config = config["sample_data"]["drifted"]

    np.random.seed(123)

    # 新しいエンティティ ID を生成（既存と重複しないように）
    start_id = num_records
    entity_ids = [f"user_{i:04d}" for i in range(start_id, start_id + num_records)]

    # ドリフトした数値特徴を生成
    ages = np.random.normal(
        data_config["age_mean"], data_config["age_std"], num_records
    )
    ages = np.clip(ages, 18, 80).astype(int)

    incomes = np.random.normal(
        data_config["income_mean"], data_config["income_std"], num_records
    )
    incomes = np.clip(incomes, 20000, 200000)

    # ドリフトしたカテゴリ特徴を生成
    categories = np.random.choice(
        data_config["categories"],
        size=num_records,
        p=data_config["category_weights"],
    )

    # タイムスタンプを生成
    timestamp = datetime.now(timezone.utc)

    df = pd.DataFrame(
        {
            "entity_id": entity_ids,
            "age": ages,
            "income": incomes,
            "category": categories,
            "feature_timestamp": [timestamp] * num_records,
        }
    )

    return df


def get_existing_stats(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> dict[str, Any]:
    """既存データの統計情報を取得する"""
    query = f"""
    SELECT
        COUNT(*) as total_records,
        AVG(age) as avg_age,
        STDDEV(age) as std_age,
        AVG(income) as avg_income,
        STDDEV(income) as std_income
    FROM `{project_id}.{dataset_id}.{table_id}`
    """

    result = client.query(query).to_dataframe()
    return result.iloc[0].to_dict()


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
        COUNT(*) as count
    FROM `{project_id}.{dataset_id}.{table_id}`
    GROUP BY category
    ORDER BY category
    """

    return client.query(query).to_dataframe()


def insert_data(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    df: pd.DataFrame,
) -> None:
    """DataFrame のデータを BigQuery テーブルに挿入する"""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description="特徴量ドリフトをシミュレートする"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="データを挿入せずにシミュレーション結果のみ表示",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("特徴量ドリフト シミュレーション")
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

    # 既存データの統計を取得
    print("\n📊 既存データの統計情報を取得中...")
    existing_stats = get_existing_stats(client, project_id, dataset_id, table_id)
    existing_categories = get_category_distribution(client, project_id, dataset_id, table_id)

    print("\n【ドリフト前のデータ】")
    print(f"  レコード数: {int(existing_stats['total_records'])}")
    print(f"  年齢: 平均 {existing_stats['avg_age']:.1f}, 標準偏差 {existing_stats['std_age']:.1f}")
    print(f"  収入: 平均 {existing_stats['avg_income']:,.0f}, 標準偏差 {existing_stats['std_income']:,.0f}")
    print(f"  カテゴリ: {dict(zip(existing_categories['category'], existing_categories['count']))}")

    # ドリフトデータを生成
    print("\n📈 ドリフトデータを生成中...")
    drifted_df = generate_drifted_data(config)

    print("\n【ドリフト後のデータ（新規生成分）】")
    print(f"  レコード数: {len(drifted_df)}")
    print(f"  年齢: 平均 {drifted_df['age'].mean():.1f}, 標準偏差 {drifted_df['age'].std():.1f}")
    print(f"  収入: 平均 {drifted_df['income'].mean():,.0f}, 標準偏差 {drifted_df['income'].std():,.0f}")
    print(f"  カテゴリ: {drifted_df['category'].value_counts().to_dict()}")

    # ドリフトの差分を計算
    print("\n" + "=" * 40)
    print("📉 ドリフト検出（予測）")
    print("=" * 40)

    initial_config = config["sample_data"]["initial"]
    drifted_config = config["sample_data"]["drifted"]

    age_drift = abs(drifted_config["age_mean"] - initial_config["age_mean"]) / initial_config["age_std"]
    income_drift = abs(drifted_config["income_mean"] - initial_config["income_mean"]) / initial_config["income_std"]

    drift_threshold = config["feature_store"]["features"][0]["drift_threshold"]

    print(f"\n  年齢ドリフト（正規化）: {age_drift:.2f}")
    print(f"    → 閾値 {drift_threshold} との比較: {'🔴 ドリフト検出' if age_drift > drift_threshold else '🟢 正常'}")

    print(f"\n  収入ドリフト（正規化）: {income_drift:.2f}")
    print(f"    → 閾値 {drift_threshold} との比較: {'🔴 ドリフト検出' if income_drift > drift_threshold else '🟢 正常'}")

    # カテゴリドリフト
    initial_categories = set(initial_config["categories"])
    drifted_categories = set(drifted_config["categories"])
    new_categories = drifted_categories - initial_categories
    if new_categories:
        print(f"\n  カテゴリ: 新しいカテゴリ追加 {new_categories}")
        print(f"    → 🔴 ドリフト検出（新カテゴリ出現）")
    else:
        # 分布の変化をチェック
        print(f"\n  カテゴリ: 分布変化あり")
        print(f"    → 🔴 ドリフト検出（分布変化）")

    if args.dry_run:
        print("\n⚠️  ドライラン: データは挿入されませんでした")
    else:
        # データを挿入
        print("\n💾 ドリフトデータを挿入中...")
        insert_data(client, project_id, dataset_id, table_id, drifted_df)
        print(f"✅ {len(drifted_df)} 件のレコードを挿入しました")

        # 挿入後の統計を取得
        print("\n📊 挿入後の統計情報を取得中...")
        new_stats = get_existing_stats(client, project_id, dataset_id, table_id)
        new_categories = get_category_distribution(client, project_id, dataset_id, table_id)

        print("\n【挿入後のデータ（全体）】")
        print(f"  レコード数: {int(new_stats['total_records'])}")
        print(f"  年齢: 平均 {new_stats['avg_age']:.1f}, 標準偏差 {new_stats['std_age']:.1f}")
        print(f"  収入: 平均 {new_stats['avg_income']:,.0f}, 標準偏差 {new_stats['std_income']:,.0f}")
        print(f"  カテゴリ: {dict(zip(new_categories['category'], new_categories['count']))}")

    print("\n" + "=" * 60)
    print("✅ ドリフトシミュレーションが完了しました")
    print("=" * 60)

    if not args.dry_run:
        print("\n次のステップ:")
        print("  1. モニタリングジョブを実行してドリフトを検出:")
        print("     python monitoring/run_monitor_job.py --wait")
        print("  2. ジョブ結果を確認:")
        print("     python monitoring/list_monitor_jobs.py")


if __name__ == "__main__":
    main()
