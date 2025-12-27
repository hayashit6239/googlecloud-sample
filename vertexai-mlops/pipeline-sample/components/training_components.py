"""
モデルトレーニング用カスタムコンポーネント

モデルのトレーニングと評価を行うコンポーネントを定義します。
"""

from kfp import dsl
from kfp.dsl import Dataset, Model, Metrics, Output, Input


@dsl.component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu:latest",
)
def train_model(
    train_dataset: Input[Dataset],
    target_column: str,
    model_type: str,
    n_estimators: int,
    max_depth: int,
    random_state: int,
    output_model: Output[Model],
    training_metrics: Output[Metrics],
) -> None:
    """
    モデルをトレーニングする

    Args:
        train_dataset: トレーニングデータセット
        target_column: ターゲットカラム名
        model_type: モデルタイプ（sklearn_random_forest, sklearn_gradient_boosting）
        n_estimators: 推定器の数
        max_depth: 最大深度
        random_state: 乱数シード
        output_model: 出力モデル
        training_metrics: トレーニング指標
    """
    import pandas as pd
    import joblib
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor

    print(f"モデルをトレーニング中: {model_type}")

    # データを読み込み
    df = pd.read_parquet(train_dataset.path)
    X = df.drop(columns=[target_column])
    y = df[target_column]

    print(f"トレーニングデータ: {X.shape}")

    # タスクタイプを判定（分類 or 回帰）
    is_classification = y.dtype == 'object' or y.nunique() < 20

    # モデルを選択
    if model_type == "sklearn_random_forest":
        if is_classification:
            model = RandomForestClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state,
            )
        else:
            model = RandomForestRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state,
            )
    elif model_type == "sklearn_gradient_boosting":
        if is_classification:
            model = GradientBoostingClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state,
            )
        else:
            model = GradientBoostingRegressor(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=random_state,
            )
    else:
        raise ValueError(f"サポートされていないモデルタイプ: {model_type}")

    print(f"モデル: {model.__class__.__name__}")
    print(f"タスク: {'分類' if is_classification else '回帰'}")

    # トレーニング
    model.fit(X, y)

    # トレーニングスコアを計算
    train_score = model.score(X, y)
    print(f"トレーニングスコア: {train_score:.4f}")

    # 特徴量重要度を取得
    feature_importance = dict(zip(X.columns, model.feature_importances_))
    top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"上位特徴量: {top_features}")

    # モデルを保存
    model_path = output_model.path + ".joblib"
    joblib.dump(model, model_path)
    output_model.uri = model_path
    print(f"モデルを保存しました: {model_path}")

    # メタデータを設定
    output_model.metadata["model_type"] = model_type
    output_model.metadata["task_type"] = "classification" if is_classification else "regression"
    output_model.metadata["n_estimators"] = n_estimators
    output_model.metadata["max_depth"] = max_depth

    # 指標を記録
    training_metrics.log_metric("train_score", train_score)
    training_metrics.log_metric("n_samples", len(X))
    training_metrics.log_metric("n_features", X.shape[1])


@dsl.component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu:latest",
)
def evaluate_model(
    test_dataset: Input[Dataset],
    model: Input[Model],
    target_column: str,
    evaluation_metrics: Output[Metrics],
) -> float:
    """
    モデルを評価する

    Args:
        test_dataset: テストデータセット
        model: トレーニング済みモデル
        target_column: ターゲットカラム名
        evaluation_metrics: 評価指標

    Returns:
        テストスコア
    """
    import pandas as pd
    import joblib
    from sklearn.metrics import (
        accuracy_score,
        precision_score,
        recall_score,
        f1_score,
        mean_squared_error,
        mean_absolute_error,
        r2_score,
    )

    print("モデルを評価中...")

    # データを読み込み
    df = pd.read_parquet(test_dataset.path)
    X = df.drop(columns=[target_column])
    y = df[target_column]

    print(f"テストデータ: {X.shape}")

    # モデルを読み込み
    # train_model で output_model.uri に .joblib パスを設定済みのため、そのまま使用
    model_path = model.path
    if not model_path.endswith(".joblib"):
        model_path = model_path + ".joblib"
    trained_model = joblib.load(model_path)
    print(f"モデルを読み込みました: {model_path}")
    print(f"モデルクラス: {trained_model.__class__.__name__}")

    # 予測
    y_pred = trained_model.predict(X)

    # タスクタイプを判定
    is_classification = model.metadata.get("task_type") == "classification"

    if is_classification:
        # 分類タスクの評価
        accuracy = accuracy_score(y, y_pred)
        precision = precision_score(y, y_pred, average='weighted', zero_division=0)
        recall = recall_score(y, y_pred, average='weighted', zero_division=0)
        f1 = f1_score(y, y_pred, average='weighted', zero_division=0)

        print(f"Accuracy: {accuracy:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall: {recall:.4f}")
        print(f"F1 Score: {f1:.4f}")

        evaluation_metrics.log_metric("accuracy", accuracy)
        evaluation_metrics.log_metric("precision", precision)
        evaluation_metrics.log_metric("recall", recall)
        evaluation_metrics.log_metric("f1_score", f1)

        return accuracy
    else:
        # 回帰タスクの評価
        mse = mean_squared_error(y, y_pred)
        mae = mean_absolute_error(y, y_pred)
        r2 = r2_score(y, y_pred)

        print(f"MSE: {mse:.4f}")
        print(f"MAE: {mae:.4f}")
        print(f"R2 Score: {r2:.4f}")

        evaluation_metrics.log_metric("mse", mse)
        evaluation_metrics.log_metric("mae", mae)
        evaluation_metrics.log_metric("r2_score", r2)

        return r2
