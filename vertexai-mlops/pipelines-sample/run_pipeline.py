#!/usr/bin/env python3
"""
パイプラインを実行するスクリプト

コンパイル済みのパイプラインを Vertex AI Pipelines で実行します。

Requires: Python 3.11+
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml
from google.cloud import aiplatform


def load_config() -> dict[str, Any]:
    """設定ファイルを読み込む（vertexai-mlops/config.yaml から pipelines セクションを取得）"""
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        root_config = yaml.safe_load(f)

    # pipelines セクションを展開してフラットな構造に変換
    pipelines_config = root_config.get("pipelines", {})
    return {
        "project_id": root_config["project_id"],
        "location": root_config["location"],
        "pipeline": {
            "name": pipelines_config.get("name", "ml-training-pipeline"),
            "pipeline_root": root_config["gcs"]["pipeline_root"],
            "staging_bucket": root_config["gcs"]["staging_bucket"],
        },
        "data": pipelines_config.get("data", {}),
        "training": pipelines_config.get("training", {}),
        "execution": pipelines_config.get("execution", {}),
        "experiments": root_config.get("experiments", {}),
    }


def run_simple_pipeline(
    config: dict[str, Any],
    message: str = "Hello, Vertex AI Pipelines!",
) -> aiplatform.PipelineJob:
    """
    シンプルパイプラインを実行する

    Args:
        config: 設定
        message: メッセージ

    Returns:
        PipelineJob インスタンス
    """
    template_path = Path(__file__).parent / "compiled" / "simple_pipeline.yaml"

    if not template_path.exists():
        raise FileNotFoundError(
            f"コンパイル済みパイプラインが見つかりません: {template_path}\n"
            "先に python compile_pipeline.py --pipeline simple を実行してください"
        )

    job = aiplatform.PipelineJob(
        display_name="simple-hello-world-pipeline",
        template_path=str(template_path),
        pipeline_root=config["pipeline"]["pipeline_root"],
        location=config["location"],
        parameter_values={
            "message": message,
        },
        enable_caching=config["execution"]["enable_caching"],
    )

    return job


def run_ml_training_pipeline(
    config: dict[str, Any],
) -> aiplatform.PipelineJob:
    """
    ML トレーニングパイプラインを実行する

    Args:
        config: 設定

    Returns:
        PipelineJob インスタンス
    """
    template_path = Path(__file__).parent / "compiled" / "ml_training_pipeline.yaml"

    if not template_path.exists():
        raise FileNotFoundError(
            f"コンパイル済みパイプラインが見つかりません: {template_path}\n"
            "先に python compile_pipeline.py --pipeline ml_training を実行してください"
        )

    job = aiplatform.PipelineJob(
        display_name=config["pipeline"]["name"],
        template_path=str(template_path),
        pipeline_root=config["pipeline"]["pipeline_root"],
        location=config["location"],
        parameter_values={
            "project_id": config["project_id"],
            "source_table": config["data"]["source_table"],
            "feature_columns": config["data"]["feature_columns"],
            "target_column": config["data"]["target_column"],
            "location": config["location"],
            "test_split_ratio": config["data"]["test_split_ratio"],
            "model_type": config["training"]["model_type"],
            "n_estimators": config["training"]["hyperparameters"]["n_estimators"],
            "max_depth": config["training"]["hyperparameters"]["max_depth"],
            "random_state": config["training"]["hyperparameters"]["random_state"],
        },
        enable_caching=config["execution"]["enable_caching"],
    )

    return job


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description="パイプラインを Vertex AI Pipelines で実行する"
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default="simple",
        choices=["ml_training", "simple"],
        help="実行するパイプライン（デフォルト: simple）",
    )
    parser.add_argument(
        "--message",
        type=str,
        default="Hello, Vertex AI Pipelines!",
        help="simple パイプラインで使用するメッセージ",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="パイプラインの完了を待機する",
    )
    parser.add_argument(
        "--experiment",
        action="store_true",
        help="Vertex AI Experiments との連携を有効にする",
    )
    parser.add_argument(
        "--no-experiment",
        action="store_true",
        help="Vertex AI Experiments との連携を無効にする",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Vertex AI Pipeline 実行")
    print("=" * 60)

    # 設定を読み込む
    config = load_config()
    project_id = config["project_id"]
    location = config["location"]

    if project_id == "your-project-id":
        print("❌ エラー: vertexai-mlops/config.yaml の project_id を設定してください")
        sys.exit(1)

    # Experiments 設定を判定
    experiments_config = config.get("experiments", {})
    if args.experiment:
        use_experiments = True
    elif args.no_experiment:
        use_experiments = False
    else:
        # デフォルトは無効（--experiment フラグで有効化）
        use_experiments = False

    experiment_name = experiments_config.get("pipeline_experiment_name", "ml-training-experiment")

    print(f"\nプロジェクト: {project_id}")
    print(f"リージョン: {location}")
    print(f"パイプライン: {args.pipeline}")
    print(f"Experiments 連携: {'有効' if use_experiments else '無効'}")
    if use_experiments:
        print(f"Experiment 名: {experiment_name}")

    # Vertex AI を初期化
    print("\n🔑 Vertex AI を初期化中...")
    aiplatform.init(
        project=project_id,
        location=location,
        staging_bucket=config["pipeline"]["staging_bucket"],
    )
    print("✅ 初期化しました")

    # パイプラインを作成
    print("\n📋 パイプラインジョブを作成中...")
    if args.pipeline == "simple":
        job = run_simple_pipeline(config, args.message)
        job_display_name = "simple-hello-world-pipeline"
    else:
        job = run_ml_training_pipeline(config)
        job_display_name = config["pipeline"]["name"]

    print(f"ジョブ名: {job_display_name}")

    # パイプラインを実行
    print("\n🚀 パイプラインを実行中...")
    service_account = config["execution"].get("service_account") or None

    # Experiments 連携の設定
    experiment = None
    if use_experiments:
        print(f"🧪 Experiment '{experiment_name}' に関連付けます...")
        # Experiment が存在しない場合は自動作成される
        experiment = experiment_name

    job.submit(
        service_account=service_account,
        experiment=experiment,
    )

    print(f"\n✅ パイプラインを送信しました")
    print(f"ジョブ名: {job.display_name}")
    print(f"リソース名: {job.resource_name}")

    # コンソール URL を表示
    console_url = (
        f"https://console.cloud.google.com/vertex-ai/pipelines/runs/"
        f"{job.resource_name.split('/')[-1]}?project={project_id}"
    )
    print(f"\n📊 Cloud Console で確認:")
    print(f"   {console_url}")

    if use_experiments:
        experiment_url = (
            f"https://console.cloud.google.com/vertex-ai/experiments/"
            f"{experiment_name}?project={project_id}"
        )
        print(f"\n🧪 Experiment:")
        print(f"   {experiment_url}")

    if args.sync:
        print("\n⏳ パイプラインの完了を待機中...")
        job.wait()
        print(f"\n✅ パイプラインが完了しました")
        print(f"状態: {job.state}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
