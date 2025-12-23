#!/usr/bin/env python3
"""
BigQuery データセットとテーブルを作成し、サンプルデータを挿入するスクリプト

このスクリプトは以下を実行します:
1. BigQuery データセットの作成
2. 特徴データ用テーブルの作成
3. サンプルデータの挿入

Requires: Python 3.11+
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import Conflict


def load_config() -> dict[str, Any]:
    """設定ファイルを読み込む"""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def generate_sample_data(config: dict[str, Any], use_drifted: bool = False) -> pd.DataFrame:
    """
    サンプル特徴データを生成する

    Args:
        config: 設定ファイルの内容
        use_drifted: True の場合、ドリフト後のデータ分布を使用

    Returns:
        生成されたサンプルデータの DataFrame
    """
    num_records = config["sample_data"]["num_records"]
    data_config = config["sample_data"]["drifted" if use_drifted else "initial"]

    np.random.seed(42 if not use_drifted else 123)

    # エンティティ ID を生成
    entity_ids = [f"user_{i:04d}" for i in range(num_records)]

    # 数値特徴を生成
    ages = np.random.normal(
        data_config["age_mean"], data_config["age_std"], num_records
    )
    ages = np.clip(ages, 18, 80).astype(int)  # 18-80歳の範囲に制限

    incomes = np.random.normal(
        data_config["income_mean"], data_config["income_std"], num_records
    )
    incomes = np.clip(incomes, 20000, 200000)  # 収入の範囲を制限

    # カテゴリ特徴を生成
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


def create_dataset(
    client: bigquery.Client, project_id: str, dataset_id: str, location: str
) -> None:
    """BigQuery データセットを作成する"""
    dataset_ref = f"{project_id}.{dataset_id}"

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    dataset.description = "Vertex AI Feature Store サンプル用データセット"

    try:
        client.create_dataset(dataset)
        print(f"✅ データセット '{dataset_ref}' を作成しました")
    except Conflict:
        print(f"ℹ️  データセット '{dataset_ref}' は既に存在します")


def create_table(
    client: bigquery.Client, project_id: str, dataset_id: str, table_id: str
) -> None:
    """BigQuery テーブルを作成する"""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    schema = [
        bigquery.SchemaField("entity_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("income", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("feature_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table.description = "ユーザー特徴データテーブル（Feature Monitoring サンプル用）"

    try:
        client.create_table(table)
        print(f"✅ テーブル '{table_ref}' を作成しました")
    except Conflict:
        print(f"ℹ️  テーブル '{table_ref}' は既に存在します。データを削除して再作成します...")
        # 既存データを削除
        query = f"DELETE FROM `{table_ref}` WHERE TRUE"
        client.query(query).result()
        print(f"✅ テーブル '{table_ref}' のデータを削除しました")


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
    job.result()  # ジョブの完了を待機

    print(f"✅ {len(df)} 件のレコードを '{table_ref}' に挿入しました")


def main():
    """メイン処理"""
    print("=" * 60)
    print("BigQuery データセット・テーブル セットアップ")
    print("=" * 60)

    # 設定を読み込む
    config = load_config()
    project_id = config["project_id"]
    location = config["location"]
    dataset_id = config["bigquery"]["dataset_id"]
    table_id = config["bigquery"]["table_id"]

    if project_id == "your-project-id":
        print("❌ エラー: config/config.yaml の project_id を設定してください")
        sys.exit(1)

    print(f"\nプロジェクト: {project_id}")
    print(f"リージョン: {location}")
    print(f"データセット: {dataset_id}")
    print(f"テーブル: {table_id}")

    # BigQuery クライアントを作成
    client = bigquery.Client(project=project_id)

    # データセットを作成
    print("\n📁 データセットを作成中...")
    create_dataset(client, project_id, dataset_id, location)

    # テーブルを作成
    print("\n📋 テーブルを作成中...")
    create_table(client, project_id, dataset_id, table_id)

    # サンプルデータを生成して挿入
    print("\n📊 サンプルデータを生成・挿入中...")
    df = generate_sample_data(config, use_drifted=False)
    insert_data(client, project_id, dataset_id, table_id, df)

    # データの統計情報を表示
    print("\n📈 挿入されたデータの統計情報:")
    print(f"  - レコード数: {len(df)}")
    print(f"  - 年齢: 平均 {df['age'].mean():.1f}, 標準偏差 {df['age'].std():.1f}")
    print(f"  - 収入: 平均 {df['income'].mean():.0f}, 標準偏差 {df['income'].std():.0f}")
    print(f"  - カテゴリ分布: {df['category'].value_counts().to_dict()}")

    print("\n" + "=" * 60)
    print("✅ BigQuery セットアップが完了しました")
    print("=" * 60)
    print("\n次のステップ: python setup/02_create_feature_group.py を実行してください")


if __name__ == "__main__":
    main()
