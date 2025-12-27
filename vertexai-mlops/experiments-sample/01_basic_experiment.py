"""
Vertex AI Experiments 基本サンプル

このスクリプトでは以下を実演します：
1. Experiment の作成
2. Experiment Run の開始
3. パラメータとメトリクスの記録
4. Run の終了と結果の確認
"""

import os
import yaml
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from google.cloud import aiplatform

# プロジェクトルートの config.yaml へのパス
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.yaml")


def load_config(config_path: str = CONFIG_PATH) -> dict:
    """設定ファイルを読み込む"""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def train_model(exp_config: dict) -> tuple:
    """モデルを学習し、メトリクスを計算する"""
    # データの読み込み
    iris = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(
        iris.data,
        iris.target,
        test_size=exp_config["data"]["test_size"],
        random_state=exp_config["data"]["random_state"],
    )

    # モデルのパラメータ
    model_params = exp_config["model"]["random_forest"]

    # モデルの学習
    model = RandomForestClassifier(
        n_estimators=model_params["n_estimators"],
        max_depth=model_params["max_depth"],
        min_samples_split=model_params["min_samples_split"],
        min_samples_leaf=model_params["min_samples_leaf"],
        random_state=model_params["random_state"],
    )
    model.fit(X_train, y_train)

    # 予測とメトリクスの計算
    y_pred = model.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, average="weighted"),
        "recall": recall_score(y_test, y_pred, average="weighted"),
        "f1_score": f1_score(y_test, y_pred, average="weighted"),
    }

    return model, model_params, metrics


def main():
    # 設定の読み込み
    config = load_config()
    exp_config = config["experiments"]  # experiments セクションを取得

    # Vertex AI の初期化
    aiplatform.init(
        project=config["project_id"],
        location=config["location"],
    )

    # Experiment の作成（既存の場合は取得）
    experiment = aiplatform.Experiment.get_or_create(
        experiment_name=exp_config["name"],
        description=exp_config["description"],
    )
    print(f"Experiment: {experiment.name}")

    # Experiment Run の開始
    with aiplatform.start_run(run="basic-run-001") as run:
        print(f"Run started: {run.name}")

        # モデルの学習
        model, params, metrics = train_model(exp_config)

        # パラメータの記録
        run.log_params(params)
        print(f"Logged params: {params}")

        # メトリクスの記録
        run.log_metrics(metrics)
        print(f"Logged metrics: {metrics}")

        # 追加のメタデータを記録
        run.log_params(
            {
                "dataset": exp_config["data"]["dataset"],
                "test_size": exp_config["data"]["test_size"],
                "model_type": "RandomForestClassifier",
            }
        )

    print("\n✅ Experiment Run が正常に完了しました")
    print(f"   Experiment: {exp_config['name']}")
    print(f"   Run: basic-run-001")
    print(
        f"   Console URL: https://console.cloud.google.com/vertex-ai/experiments/{exp_config['name']}/runs?project={config['project_id']}"
    )


if __name__ == "__main__":
    main()
