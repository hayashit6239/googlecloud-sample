"""
ML トレーニングパイプライン

BigQuery からデータを読み込み、前処理、トレーニング、評価を行う
エンドツーエンドの ML パイプラインを定義します。
"""

from kfp import dsl
from kfp.dsl import PipelineTask

# コンポーネントをインポート
import sys
from pathlib import Path

# 親ディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from components.data_components import (
    load_data_from_bigquery,
    split_data,
    preprocess_data,
)
from components.training_components import (
    train_model,
    evaluate_model,
)


@dsl.pipeline(
    name="ml-training-pipeline",
    description="Feature Store のユーザー特徴データからカテゴリを予測する分類モデルパイプライン",
)
def ml_training_pipeline(
    project_id: str,
    source_table: str,
    feature_columns: list,
    target_column: str,
    location: str = "asia-northeast1",
    test_split_ratio: float = 0.2,
    model_type: str = "sklearn_random_forest",
    n_estimators: int = 100,
    max_depth: int = 10,
    random_state: int = 42,
) -> None:
    """
    ML トレーニングパイプライン（分類モデル）

    Feature Store のユーザー特徴データ（age, income）から
    カテゴリ（A, B, C）を予測する分類モデルをトレーニング・評価します。

    Args:
        project_id: GCP プロジェクト ID
        source_table: BigQuery ソーステーブル（例: project.feature_store.user_features）
        feature_columns: 特徴量カラムのリスト（例: ["age", "income"]）
        target_column: ターゲットカラム名（例: "category"）
        location: BigQuery データセットのリージョン
        test_split_ratio: テストデータの割合
        model_type: モデルタイプ（sklearn_random_forest, sklearn_gradient_boosting）
        n_estimators: 推定器の数
        max_depth: 最大深度
        random_state: 乱数シード
    """
    # Step 1: BigQuery からデータを読み込み
    load_data_task = load_data_from_bigquery(
        project_id=project_id,
        source_table=source_table,
        feature_columns=feature_columns,
        target_column=target_column,
        location=location,
    ).set_display_name("データ読み込み")

    # Step 2: データを分割
    split_data_task = split_data(
        input_dataset=load_data_task.outputs["output_dataset"],
        target_column=target_column,
        test_split_ratio=test_split_ratio,
    ).set_display_name("データ分割")

    # Step 3: トレーニングデータの前処理
    preprocess_train_task = preprocess_data(
        input_dataset=split_data_task.outputs["train_dataset"],
        target_column=target_column,
    ).set_display_name("トレーニングデータ前処理")

    # Step 4: テストデータの前処理
    preprocess_test_task = preprocess_data(
        input_dataset=split_data_task.outputs["test_dataset"],
        target_column=target_column,
    ).set_display_name("テストデータ前処理")

    # Step 5: モデルトレーニング
    train_model_task = train_model(
        train_dataset=preprocess_train_task.outputs["output_dataset"],
        target_column=target_column,
        model_type=model_type,
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
    ).set_display_name("モデルトレーニング")

    # Step 6: モデル評価
    evaluate_model_task = evaluate_model(
        test_dataset=preprocess_test_task.outputs["output_dataset"],
        model=train_model_task.outputs["output_model"],
        target_column=target_column,
    ).set_display_name("モデル評価")


@dsl.pipeline(
    name="simple-hello-world-pipeline",
    description="シンプルな Hello World パイプライン",
)
def simple_pipeline(
    message: str = "Hello, Vertex AI Pipelines!",
) -> str:
    """
    シンプルな Hello World パイプライン

    Args:
        message: 出力するメッセージ

    Returns:
        処理結果のメッセージ
    """

    @dsl.component(base_image="gcr.io/deeplearning-platform-release/base-cpu:latest")
    def hello_world(text: str) -> str:
        """メッセージを出力する"""
        print(f"Message: {text}")
        return f"Processed: {text}"

    @dsl.component(base_image="gcr.io/deeplearning-platform-release/base-cpu:latest")
    def add_timestamp(text: str) -> str:
        """タイムスタンプを追加する"""
        from datetime import datetime

        timestamp = datetime.now().isoformat()
        result = f"{text} at {timestamp}"
        print(result)
        return result

    # パイプラインステップ
    hello_task = hello_world(text=message).set_display_name("Hello World")
    timestamp_task = add_timestamp(text=hello_task.output).set_display_name(
        "Add Timestamp"
    )

    return timestamp_task.output
