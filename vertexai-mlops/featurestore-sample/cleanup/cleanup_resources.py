#!/usr/bin/env python3
"""
作成したリソースをクリーンアップするスクリプト

このスクリプトは以下を削除します:
1. FeatureMonitor
2. Feature
3. FeatureGroup
4. BigQuery テーブル（オプション）
5. BigQuery データセット（オプション）

Requires: Python 3.11+
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Any

import yaml
from google.api_core import exceptions
from google.cloud import aiplatform, bigquery
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


def delete_feature_monitor(
    feature_group: feature_store.FeatureGroup,
    monitor_name: str,
) -> bool:
    """FeatureMonitor を削除する"""
    try:
        monitor = feature_group.get_feature_monitor(monitor_name)
        monitor.delete()
        print(f"✅ FeatureMonitor '{monitor_name}' を削除しました")
        return True
    except exceptions.NotFound:
        print(f"ℹ️  FeatureMonitor '{monitor_name}' は存在しません")
        return True
    except Exception as e:
        print(f"❌ FeatureMonitor の削除に失敗しました: {e}")
        return False


def delete_features(
    feature_group: feature_store.FeatureGroup,
) -> bool:
    """FeatureGroup 内の全ての Feature を削除する"""
    try:
        features = list(feature_group.list_features())
        for feature in features:
            feature_name = feature.resource_name.split("/")[-1]
            print(f"   🗑️  Feature '{feature_name}' を削除中...")
            feature.delete()
            print(f"   ✅ Feature '{feature_name}' を削除しました")
        return True
    except Exception as e:
        print(f"❌ Feature の削除中にエラーが発生しました: {e}")
        return False


def delete_feature_group(
    feature_group_name: str,
) -> bool:
    """FeatureGroup を削除する"""
    try:
        feature_group = get_feature_group(feature_group_name)

        if feature_group is None:
            print(f"ℹ️  FeatureGroup '{feature_group_name}' は存在しません")
            return True

        # まず Feature を削除
        print("   📊 Feature を削除中...")
        delete_features(feature_group)

        # 少し待機
        time.sleep(2)

        # FeatureGroup を削除
        print(f"   🗑️  FeatureGroup '{feature_group_name}' を削除中...")
        feature_group.delete()
        print(f"✅ FeatureGroup '{feature_group_name}' を削除しました")
        return True
    except exceptions.NotFound:
        print(f"ℹ️  FeatureGroup '{feature_group_name}' は存在しません")
        return True
    except Exception as e:
        print(f"❌ FeatureGroup の削除に失敗しました: {e}")
        return False


def delete_bigquery_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> bool:
    """BigQuery テーブルを削除する"""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        client.delete_table(table_ref)
        print(f"✅ BigQuery テーブル '{table_ref}' を削除しました")
        return True
    except exceptions.NotFound:
        print(f"ℹ️  BigQuery テーブル '{table_ref}' は存在しません")
        return True
    except Exception as e:
        print(f"❌ BigQuery テーブルの削除に失敗しました: {e}")
        return False


def delete_bigquery_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
) -> bool:
    """BigQuery データセットを削除する"""
    dataset_ref = f"{project_id}.{dataset_id}"

    try:
        client.delete_dataset(dataset_ref, delete_contents=True)
        print(f"✅ BigQuery データセット '{dataset_ref}' を削除しました")
        return True
    except exceptions.NotFound:
        print(f"ℹ️  BigQuery データセット '{dataset_ref}' は存在しません")
        return True
    except Exception as e:
        print(f"❌ BigQuery データセットの削除に失敗しました: {e}")
        return False


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description="作成したリソースをクリーンアップする"
    )
    parser.add_argument(
        "--include-bigquery",
        action="store_true",
        help="BigQuery のテーブルとデータセットも削除する",
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="確認なしで削除を実行する",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("リソース クリーンアップ")
    print("=" * 60)

    # 設定を読み込む
    config = load_config()
    project_id = config["project_id"]
    location = config["location"]
    dataset_id = config["bigquery"]["dataset_id"]
    table_id = config["bigquery"]["table_id"]
    feature_group_name = config["feature_store"]["feature_group_name"]
    monitor_name = config["feature_monitor"]["name"]

    if project_id == "your-project-id":
        print("❌ エラー: config/config.yaml の project_id を設定してください")
        sys.exit(1)

    print(f"\nプロジェクト: {project_id}")
    print(f"リージョン: {location}")
    print(f"\n削除対象リソース:")
    print(f"  - FeatureMonitor: {monitor_name}")
    print(f"  - FeatureGroup: {feature_group_name}")
    if args.include_bigquery:
        print(f"  - BigQuery テーブル: {project_id}.{dataset_id}.{table_id}")
        print(f"  - BigQuery データセット: {project_id}.{dataset_id}")

    # 確認
    if not args.force:
        print("\n⚠️  上記のリソースを削除します。")
        response = input("続行しますか？ [y/N]: ")
        if response.lower() != "y":
            print("キャンセルしました")
            sys.exit(0)

    # Vertex AI を初期化
    print("\n🔑 Vertex AI を初期化中...")
    aiplatform.init(project=project_id, location=location)
    print("✅ 初期化しました")

    success = True

    # FeatureGroup を取得
    feature_group = get_feature_group(feature_group_name)

    # FeatureMonitor を削除
    print("\n🗑️  FeatureMonitor を削除中...")
    if feature_group is not None:
        if not delete_feature_monitor(feature_group, monitor_name):
            success = False
    else:
        print(f"ℹ️  FeatureGroup '{feature_group_name}' が存在しないため、FeatureMonitor の削除をスキップ")

    # 少し待機
    time.sleep(2)

    # FeatureGroup を削除
    print("\n🗑️  FeatureGroup を削除中...")
    if not delete_feature_group(feature_group_name):
        success = False

    # BigQuery リソースを削除（オプション）
    if args.include_bigquery:
        client = bigquery.Client(project=project_id)

        print("\n🗑️  BigQuery リソースを削除中...")

        # テーブルを削除
        if not delete_bigquery_table(client, project_id, dataset_id, table_id):
            success = False

        # データセットを削除
        if not delete_bigquery_dataset(client, project_id, dataset_id):
            success = False

    print("\n" + "=" * 60)
    if success:
        print("✅ クリーンアップが完了しました")
    else:
        print("⚠️  一部のリソースの削除に失敗しました")
    print("=" * 60)


if __name__ == "__main__":
    main()
