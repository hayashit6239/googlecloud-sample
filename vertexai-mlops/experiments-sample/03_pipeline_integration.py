"""
Vertex AI Experiments と Pipelines の連携サンプル

このスクリプトでは、Vertex AI Pipelines 内で
Experiment を使用してメトリクスを記録する方法を実演します。

パイプラインの各コンポーネントで記録されたメトリクスは
Experiment Run に自動的に関連付けられます。
"""

import os
import yaml
from kfp import dsl
from kfp.dsl import component, Output, Metrics, Model, Dataset
from google.cloud import aiplatform

# プロジェクトルートの config.yaml へのパス
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.yaml")


def load_config(config_path: str = CONFIG_PATH) -> dict:
    """設定ファイルを読み込む"""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# =============================================================================
# パイプラインコンポーネント
# =============================================================================


@component(
    base_image="python:3.10",
    packages_to_install=["scikit-learn==1.3.0", "pandas==2.0.3"],
)
def prepare_data(
    test_size: float,
    random_state: int,
    train_dataset: Output[Dataset],
    test_dataset: Output[Dataset],
    metrics: Output[Metrics],
):
    """データを準備し、train/test に分割する"""
    import pickle
    from sklearn.datasets import load_iris
    from sklearn.model_selection import train_test_split

    # データの読み込み
    iris = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(
        iris.data,
        iris.target,
        test_size=test_size,
        random_state=random_state,
    )

    # データセットの保存
    with open(train_dataset.path, "wb") as f:
        pickle.dump({"X": X_train, "y": y_train}, f)

    with open(test_dataset.path, "wb") as f:
        pickle.dump({"X": X_test, "y": y_test}, f)

    # メトリクスの記録（Experiment に自動連携）
    metrics.log_metric("train_samples", len(X_train))
    metrics.log_metric("test_samples", len(X_test))
    metrics.log_metric("n_features", X_train.shape[1])
    metrics.log_metric("n_classes", len(set(iris.target)))


@component(
    base_image="python:3.10",
    packages_to_install=["scikit-learn==1.3.0", "google-cloud-aiplatform==1.38.0"],
)
def train_model(
    train_dataset: Dataset,
    n_estimators: int,
    max_depth: int,
    random_state: int,
    model_artifact: Output[Model],
    metrics: Output[Metrics],
):
    """モデルを学習する"""
    import pickle
    from sklearn.ensemble import RandomForestClassifier
    from google.cloud import aiplatform

    # データの読み込み
    with open(train_dataset.path, "rb") as f:
        data = pickle.load(f)

    X_train, y_train = data["X"], data["y"]

    # autolog を有効化（パイプライン内でも動作）
    aiplatform.autolog()

    # モデルの学習
    model = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
    )
    model.fit(X_train, y_train)

    # autolog を無効化
    aiplatform.autolog(disable=True)

    # モデルの保存
    with open(model_artifact.path, "wb") as f:
        pickle.dump(model, f)

    # ハイパーパラメータをメトリクスとして記録
    metrics.log_metric("n_estimators", n_estimators)
    metrics.log_metric("max_depth", max_depth)
    metrics.log_metric("n_features_in", model.n_features_in_)


@component(
    base_image="python:3.10",
    packages_to_install=["scikit-learn==1.3.0"],
)
def evaluate_model(
    model_artifact: Model,
    test_dataset: Dataset,
    metrics: Output[Metrics],
):
    """モデルを評価する"""
    import pickle
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

    # モデルの読み込み
    with open(model_artifact.path, "rb") as f:
        model = pickle.load(f)

    # テストデータの読み込み
    with open(test_dataset.path, "rb") as f:
        data = pickle.load(f)

    X_test, y_test = data["X"], data["y"]

    # 予測
    y_pred = model.predict(X_test)

    # メトリクスの計算と記録
    metrics.log_metric("accuracy", accuracy_score(y_test, y_pred))
    metrics.log_metric("precision", precision_score(y_test, y_pred, average="weighted"))
    metrics.log_metric("recall", recall_score(y_test, y_pred, average="weighted"))
    metrics.log_metric("f1_score", f1_score(y_test, y_pred, average="weighted"))


# =============================================================================
# パイプライン定義
# =============================================================================


@dsl.pipeline(
    name="experiment-integration-pipeline",
    description="Vertex AI Experiments と連携する ML パイプライン",
)
def ml_pipeline_with_experiment(
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 100,
    max_depth: int = 10,
):
    """Experiment 連携パイプライン"""

    # データ準備
    prepare_task = prepare_data(
        test_size=test_size,
        random_state=random_state,
    )

    # モデル学習
    train_task = train_model(
        train_dataset=prepare_task.outputs["train_dataset"],
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
    )

    # モデル評価
    evaluate_model(
        model_artifact=train_task.outputs["model_artifact"],
        test_dataset=prepare_task.outputs["test_dataset"],
    )


# =============================================================================
# 実行スクリプト
# =============================================================================


def compile_pipeline(output_path: str = "compiled/experiment_pipeline.yaml"):
    """パイプラインをコンパイルする"""
    from kfp import compiler
    import os

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    compiler.Compiler().compile(
        pipeline_func=ml_pipeline_with_experiment,
        package_path=output_path,
    )
    print(f"✅ Pipeline compiled to: {output_path}")


def run_pipeline_with_experiment(config: dict, exp_config: dict):
    """パイプラインを Experiment と連携して実行する"""

    # Vertex AI の初期化
    aiplatform.init(
        project=config["project_id"],
        location=config["location"],
    )

    # Experiment の作成または取得
    experiment = aiplatform.Experiment.get_or_create(
        experiment_name=exp_config["pipeline_experiment_name"],
        description="Pipeline integration experiment",
    )

    # パイプラインジョブの作成
    job = aiplatform.PipelineJob(
        display_name="experiment-pipeline-run",
        template_path="compiled/experiment_pipeline.yaml",
        parameter_values={
            "test_size": exp_config["data"]["test_size"],
            "random_state": exp_config["data"]["random_state"],
            "n_estimators": exp_config["model"]["random_forest"]["n_estimators"],
            "max_depth": exp_config["model"]["random_forest"]["max_depth"],
        },
    )

    # Experiment Run として実行
    job.submit(
        experiment=experiment,
    )

    print(f"✅ Pipeline submitted with experiment: {experiment.name}")
    print(f"   Pipeline Job: {job.display_name}")
    print(f"   Resource Name: {job.resource_name}")

    return job


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Experiment と Pipeline の連携サンプル"
    )
    parser.add_argument(
        "--compile-only",
        action="store_true",
        help="パイプラインのコンパイルのみ実行",
    )
    parser.add_argument(
        "--config",
        default=CONFIG_PATH,
        help="設定ファイルのパス",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    exp_config = config["experiments"]  # experiments セクションを取得

    if args.compile_only:
        compile_pipeline()
    else:
        compile_pipeline()
        run_pipeline_with_experiment(config, exp_config)


if __name__ == "__main__":
    main()
