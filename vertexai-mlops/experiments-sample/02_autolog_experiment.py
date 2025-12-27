"""
Vertex AI Experiments autolog サンプル

このスクリプトでは aiplatform.autolog() を使用して
自動的にパラメータとメトリクスを記録する方法を実演します。

autolog() は以下のフレームワークをサポートしています：
- scikit-learn
- XGBoost
- TensorFlow/Keras（ただし Keras 3.0 以降は非サポート）
"""

import os
import yaml
from sklearn.datasets import load_iris, load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from google.cloud import aiplatform

# プロジェクトルートの config.yaml へのパス
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.yaml")


def load_config(config_path: str = CONFIG_PATH) -> dict:
    """設定ファイルを読み込む"""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_sklearn_autolog_example(exp_config: dict):
    """
    scikit-learn の autolog サンプル

    autolog() を有効化すると、以下が自動記録されます：
    - モデルのハイパーパラメータ（n_estimators, max_depth 等）
    - 学習メトリクス（accuracy 等）
    - モデルのクラス名
    """
    print("=" * 60)
    print("scikit-learn autolog サンプル")
    print("=" * 60)

    # データの準備
    iris = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(
        iris.data,
        iris.target,
        test_size=exp_config["data"]["test_size"],
        random_state=exp_config["data"]["random_state"],
    )

    # autolog の有効化（Experiment 内で実行）
    aiplatform.autolog()

    # 複数のモデルを試す
    models = [
        ("RandomForest", RandomForestClassifier(n_estimators=50, max_depth=5)),
        ("GradientBoosting", GradientBoostingClassifier(n_estimators=50, max_depth=3)),
        ("LogisticRegression", LogisticRegression(max_iter=200)),
    ]

    for model_name, model in models:
        print(f"\n🔄 Training {model_name}...")

        # モデルの学習（autolog により自動記録）
        model.fit(X_train, y_train)

        # スコアの計算
        score = model.score(X_test, y_test)
        print(f"   Accuracy: {score:.4f}")

    # autolog の無効化
    aiplatform.autolog(disable=True)

    print("\n✅ scikit-learn autolog サンプル完了")


def run_multiple_experiments_example():
    """
    複数の Experiment Run で autolog を使用するサンプル

    ハイパーパラメータチューニングのような
    複数の実験を効率的に記録できます。
    """
    print("\n" + "=" * 60)
    print("ハイパーパラメータ探索 with autolog")
    print("=" * 60)

    # データの準備
    cancer = load_breast_cancer()
    X_train, X_test, y_train, y_test = train_test_split(
        cancer.data,
        cancer.target,
        test_size=0.2,
        random_state=42,
    )

    # ハイパーパラメータの組み合わせ
    param_grid = [
        {"n_estimators": 50, "max_depth": 3},
        {"n_estimators": 100, "max_depth": 5},
        {"n_estimators": 100, "max_depth": 10},
        {"n_estimators": 200, "max_depth": 5},
    ]

    best_score = 0
    best_params = None

    for i, params in enumerate(param_grid):
        run_name = f"hp-search-{i+1:03d}"

        with aiplatform.start_run(run=run_name) as run:
            # autolog 有効化
            aiplatform.autolog()

            print(f"\n🔄 Run {run_name}: {params}")

            # モデルの学習
            model = RandomForestClassifier(**params, random_state=42)
            model.fit(X_train, y_train)

            # スコアの計算と記録
            score = model.score(X_test, y_test)
            run.log_metrics({"test_accuracy": score})

            print(f"   Test Accuracy: {score:.4f}")

            if score > best_score:
                best_score = score
                best_params = params

            # autolog 無効化
            aiplatform.autolog(disable=True)

    print(f"\n🏆 Best Score: {best_score:.4f}")
    print(f"   Best Params: {best_params}")


def main():
    # 設定の読み込み
    config = load_config()
    exp_config = config["experiments"]  # experiments セクションを取得

    # Vertex AI の初期化
    aiplatform.init(
        project=config["project_id"],
        location=config["location"],
        experiment=exp_config["name"],
    )

    print(f"Project: {config['project_id']}")
    print(f"Location: {config['location']}")
    print(f"Experiment: {exp_config['name']}")

    # サンプル 1: scikit-learn autolog
    with aiplatform.start_run(run="autolog-sklearn-demo"):
        run_sklearn_autolog_example(exp_config)

    # サンプル 2: 複数の Experiment Run
    run_multiple_experiments_example()

    print("\n" + "=" * 60)
    print("✅ すべての autolog サンプルが完了しました")
    print("=" * 60)
    print(
        f"\nConsole URL: https://console.cloud.google.com/vertex-ai/experiments/{exp_config['name']}/runs?project={config['project_id']}"
    )


if __name__ == "__main__":
    main()
