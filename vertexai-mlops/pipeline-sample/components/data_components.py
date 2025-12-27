"""
データ処理用カスタムコンポーネント

BigQuery からのデータ読み込み、分割、前処理を行うコンポーネントを定義します。
"""

from kfp import dsl
from kfp.dsl import Dataset, Output, Input


@dsl.component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu:latest",
    packages_to_install=[
        "google-cloud-bigquery>=3.25.0",
        "db-dtypes>=1.2.0",
    ],
)
def load_data_from_bigquery(
    project_id: str,
    source_table: str,
    feature_columns: list,
    target_column: str,
    location: str,
    output_dataset: Output[Dataset],
) -> None:
    """
    BigQuery からデータを読み込む

    Args:
        project_id: GCP プロジェクト ID
        source_table: BigQuery テーブル（project.dataset.table 形式）
        feature_columns: 特徴量カラムのリスト
        target_column: ターゲットカラム名
        location: BigQuery データセットのリージョン
        output_dataset: 出力データセット
    """
    from google.cloud import bigquery
    import pandas as pd

    print(f"BigQuery からデータを読み込み中: {source_table}")
    print(f"リージョン: {location}")

    client = bigquery.Client(project=project_id, location=location)

    # カラムを選択してクエリを構築
    columns = feature_columns + [target_column]
    columns_str = ", ".join(columns)
    query = f"SELECT {columns_str} FROM `{source_table}`"

    print(f"クエリ: {query}")

    # データを読み込み
    df = client.query(query).to_dataframe()

    print(f"読み込んだデータ: {len(df)} 行, {len(df.columns)} 列")
    print(f"カラム: {list(df.columns)}")

    # Parquet 形式で保存
    df.to_parquet(output_dataset.path, index=False)

    print(f"データを保存しました: {output_dataset.path}")


@dsl.component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu:latest",
)
def split_data(
    input_dataset: Input[Dataset],
    target_column: str,
    test_split_ratio: float,
    train_dataset: Output[Dataset],
    test_dataset: Output[Dataset],
) -> None:
    """
    データをトレーニングセットとテストセットに分割する

    Args:
        input_dataset: 入力データセット
        target_column: ターゲットカラム名
        test_split_ratio: テストデータの割合
        train_dataset: トレーニングデータセット出力
        test_dataset: テストデータセット出力
    """
    import pandas as pd
    from sklearn.model_selection import train_test_split

    print(f"データを分割中: テスト割合 = {test_split_ratio}")

    # データを読み込み
    df = pd.read_parquet(input_dataset.path)
    print(f"入力データ: {len(df)} 行")

    # 特徴量とターゲットを分離
    X = df.drop(columns=[target_column])
    y = df[target_column]

    # データを分割
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=test_split_ratio,
        random_state=42,
        stratify=y if y.dtype == 'object' or y.nunique() < 20 else None,
    )

    # トレーニングデータを保存
    train_df = X_train.copy()
    train_df[target_column] = y_train
    train_df.to_parquet(train_dataset.path, index=False)
    print(f"トレーニングデータ: {len(train_df)} 行")

    # テストデータを保存
    test_df = X_test.copy()
    test_df[target_column] = y_test
    test_df.to_parquet(test_dataset.path, index=False)
    print(f"テストデータ: {len(test_df)} 行")


@dsl.component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu:latest",
)
def preprocess_data(
    input_dataset: Input[Dataset],
    target_column: str,
    output_dataset: Output[Dataset],
) -> None:
    """
    データの前処理を行う

    Args:
        input_dataset: 入力データセット
        target_column: ターゲットカラム名
        output_dataset: 前処理済みデータセット出力
    """
    import pandas as pd
    from sklearn.preprocessing import StandardScaler
    import numpy as np

    print("データの前処理を実行中...")

    # データを読み込み
    df = pd.read_parquet(input_dataset.path)

    # 特徴量とターゲットを分離
    X = df.drop(columns=[target_column])
    y = df[target_column]

    # 数値カラムのみを標準化
    numeric_cols = X.select_dtypes(include=[np.number]).columns.tolist()

    if numeric_cols:
        scaler = StandardScaler()
        X[numeric_cols] = scaler.fit_transform(X[numeric_cols])
        print(f"標準化したカラム: {numeric_cols}")

    # 欠損値を処理
    missing_before = X.isnull().sum().sum()
    X = X.fillna(X.median(numeric_only=True))
    X = X.fillna("unknown")  # カテゴリカル変数用
    missing_after = X.isnull().sum().sum()
    print(f"欠損値: {missing_before} -> {missing_after}")

    # 結果を保存
    result_df = X.copy()
    result_df[target_column] = y
    result_df.to_parquet(output_dataset.path, index=False)

    print(f"前処理済みデータを保存しました: {len(result_df)} 行")
