#!/usr/bin/env python3
"""
FeatureGroup と Feature を作成するスクリプト

このスクリプトは以下を実行します:
1. BigQuery テーブルをソースとする FeatureGroup の作成
2. 特徴量（Feature）の登録

Requires: Python 3.11+
"""

import sys
import time
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


def create_feature_group(
    feature_group_name: str,
    bq_table_uri: str,
    entity_id_columns: list[str],
) -> feature_store.FeatureGroup:
    """
    FeatureGroup を作成する

    Args:
        feature_group_name: FeatureGroup の名前
        bq_table_uri: BigQuery テーブルの URI
        entity_id_columns: エンティティ ID カラムのリスト

    Returns:
        作成された FeatureGroup
    """
    fg = feature_store.FeatureGroup.create(
        name=feature_group_name,
        source=feature_store.utils.FeatureGroupBigQuerySource(
            uri=bq_table_uri,
            entity_id_columns=entity_id_columns,
        ),
    )
    return fg


def get_feature(
    feature_group: feature_store.FeatureGroup,
    feature_name: str,
) -> feature_store.Feature | None:
    """既存の Feature を取得する"""
    try:
        return feature_group.get_feature(feature_name)
    except Exception:
        return None


def create_feature(
    feature_group: feature_store.FeatureGroup,
    feature_name: str,
    description: str,
) -> feature_store.Feature:
    """
    Feature を作成する

    Args:
        feature_group: 親となる FeatureGroup
        feature_name: Feature の名前
        description: Feature の説明

    Returns:
        作成された Feature
    """
    feature = feature_group.create_feature(
        name=feature_name,
        description=description,
    )
    return feature


def list_features(
    feature_group: feature_store.FeatureGroup,
) -> list[feature_store.Feature]:
    """FeatureGroup 内の Feature 一覧を取得する"""
    return list(feature_group.list_features())


def main():
    """メイン処理"""
    print("=" * 60)
    print("FeatureGroup・Feature セットアップ")
    print("=" * 60)

    # 設定を読み込む
    config = load_config()
    project_id = config["project_id"]
    location = config["location"]
    dataset_id = config["bigquery"]["dataset_id"]
    table_id = config["bigquery"]["table_id"]
    feature_group_name = config["feature_store"]["feature_group_name"]
    features_config = config["feature_store"]["features"]

    if project_id == "your-project-id":
        print("❌ エラー: config/config.yaml の project_id を設定してください")
        sys.exit(1)

    print(f"\nプロジェクト: {project_id}")
    print(f"リージョン: {location}")
    print(f"FeatureGroup: {feature_group_name}")

    # Vertex AI を初期化
    print("\n🔑 Vertex AI を初期化中...")
    aiplatform.init(project=project_id, location=location)
    print("✅ 初期化しました")

    # BigQuery テーブル URI を構築
    bq_table_uri = f"bq://{project_id}.{dataset_id}.{table_id}"

    # FeatureGroup を作成
    print("\n📁 FeatureGroup を作成中...")
    existing_fg = get_feature_group(feature_group_name)

    if existing_fg is not None:
        print(f"ℹ️  FeatureGroup '{feature_group_name}' は既に存在します")
        print(f"   リソース名: {existing_fg.resource_name}")
        fg = existing_fg
    else:
        print(f"📦 FeatureGroup '{feature_group_name}' を作成中...")
        print(f"   BigQuery ソース: {bq_table_uri}")

        try:
            fg = create_feature_group(
                feature_group_name=feature_group_name,
                bq_table_uri=bq_table_uri,
                entity_id_columns=["entity_id"],
            )
            print(f"✅ FeatureGroup '{feature_group_name}' を作成しました")
            print(f"   リソース名: {fg.resource_name}")
        except Exception as e:
            print(f"❌ FeatureGroup の作成に失敗しました: {e}")
            sys.exit(1)

    # 少し待機（API の反映を待つ）
    time.sleep(2)

    # Feature を作成
    print("\n📊 Feature を作成中...")
    for feature_config in features_config:
        feature_name = feature_config["name"]
        description = f"{feature_name} 特徴量（タイプ: {feature_config['type']}）"

        existing_feature = get_feature(fg, feature_name)

        if existing_feature is not None:
            print(f"ℹ️  Feature '{feature_name}' は既に存在します")
        else:
            print(f"   📊 Feature '{feature_name}' を作成中...")
            try:
                create_feature(
                    feature_group=fg,
                    feature_name=feature_name,
                    description=description,
                )
                print(f"   ✅ Feature '{feature_name}' を作成しました")
            except Exception as e:
                print(f"   ❌ Feature '{feature_name}' の作成に失敗しました: {e}")

    # 少し待機
    time.sleep(2)

    # 作成された Feature の一覧を表示
    print("\n📋 登録された Feature 一覧:")
    features = list_features(fg)
    for f in features:
        feature_id = f.resource_name.split("/")[-1] if f.resource_name else "N/A"
        print(f"   - {feature_id}")

    print("\n" + "=" * 60)
    print("✅ FeatureGroup・Feature セットアップが完了しました")
    print("=" * 60)
    print(
        "\n次のステップ: python setup/03_create_feature_monitor.py を実行してください"
    )


if __name__ == "__main__":
    main()
