# Vertex AI Experiments サンプル

Vertex AI Experiments を使用した実験追跡のサンプルコード集です。

## 概要

Vertex AI Experiments は、機械学習の実験を追跡・管理するためのサービスです。
このディレクトリには、以下の機能を実演するサンプルが含まれています：

- 基本的な Experiment の作成と Run の管理
- autolog() による自動ログ記録
- Pipelines との連携

## 前提条件

- Google Cloud プロジェクト
- 適切な IAM 権限（Vertex AI User 以上）
- Python 3.10+
- gcloud CLI（認証済み）

## セットアップ

### 1. 依存関係のインストール

```bash
pip install -r requirements.txt
```

### 2. 設定ファイルの編集

親ディレクトリの `vertexai-mlops/config.yaml` を編集し、プロジェクト固有の設定を行います：

```yaml
project_id: "your-project-id"
location: "asia-northeast1"

experiments:
  name: "sample-experiment"
  description: "Vertex AI Experiments サンプル"
  # ...
```

### 3. 認証

```bash
gcloud auth application-default login
```

## サンプルの実行

### 01_basic_experiment.py - 基本サンプル

Experiment の作成、Run の開始、パラメータ/メトリクスの記録を行います。

```bash
python 01_basic_experiment.py
```

**学習できること：**
- `aiplatform.init()` での初期化
- `Experiment.get_or_create()` での Experiment 作成
- `aiplatform.start_run()` コンテキストマネージャの使用
- `run.log_params()` / `run.log_metrics()` でのデータ記録

### 02_autolog_experiment.py - autolog サンプル

`autolog()` を使用して、モデルの学習を自動的に記録します。

```bash
python 02_autolog_experiment.py
```

**学習できること：**
- `aiplatform.autolog()` の有効化/無効化
- 複数モデルの自動ログ記録
- ハイパーパラメータ探索での活用

**サポートされるフレームワーク：**
| フレームワーク | サポート状況 |
|---------------|-------------|
| scikit-learn  | ✅ 完全サポート |
| XGBoost       | ✅ 完全サポート |
| TensorFlow    | ✅ Keras 2.x のみ |
| PyTorch       | ❌ 非サポート |

### 03_pipeline_integration.py - パイプライン連携サンプル

Vertex AI Pipelines と Experiments を連携させます。

```bash
# コンパイルのみ
python 03_pipeline_integration.py --compile-only

# コンパイル + 実行
python 03_pipeline_integration.py
```

**学習できること：**
- パイプラインコンポーネントでの `Metrics` 出力の使用
- `PipelineJob.submit(experiment=...)` での Experiment 連携
- パイプライン実行結果の Experiment Run への自動関連付け

## ディレクトリ構造

```
vertexai-mlops/
├── config.yaml                  # 共通設定ファイル
└── experiments-sample/
    ├── README.md                    # このファイル
    ├── requirements.txt             # 依存関係
    ├── 01_basic_experiment.py       # 基本サンプル
    ├── 02_autolog_experiment.py     # autolog サンプル
    ├── 03_pipeline_integration.py   # パイプライン連携サンプル
    └── compiled/                    # コンパイル済みパイプライン（自動生成）
```

## 主要な API

### Experiment の作成

```python
from google.cloud import aiplatform

aiplatform.init(project="your-project", location="asia-northeast1")

experiment = aiplatform.Experiment.get_or_create(
    experiment_name="my-experiment",
    description="実験の説明",
)
```

### Run の開始とデータ記録

```python
with aiplatform.start_run(run="run-001") as run:
    # パラメータの記録
    run.log_params({"learning_rate": 0.01, "epochs": 100})

    # メトリクスの記録
    run.log_metrics({"accuracy": 0.95, "loss": 0.05})

    # 時系列メトリクスの記録
    for epoch in range(100):
        run.log_time_series_metrics({"loss": current_loss}, step=epoch)
```

### autolog の使用

```python
# 有効化
aiplatform.autolog()

# モデルの学習（自動的に記録される）
model.fit(X_train, y_train)

# 無効化
aiplatform.autolog(disable=True)
```

## 結果の確認

実験結果は Google Cloud Console で確認できます：

```
https://console.cloud.google.com/vertex-ai/experiments
```

または Python SDK から：

```python
# 実験の一覧取得
experiments = aiplatform.Experiment.list()

# 特定の実験の Run を取得
experiment = aiplatform.Experiment("my-experiment")
runs = experiment.get_experiment_df()
```

## トラブルシューティング

### 認証エラー

```bash
# アプリケーションデフォルト認証の設定
gcloud auth application-default login

# サービスアカウントを使用する場合
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"
```

### 権限エラー

必要な IAM ロール：
- `roles/aiplatform.user` - Vertex AI の基本操作
- `roles/storage.objectViewer` - GCS からのデータ読み取り（必要な場合）

### autolog が動作しない

- フレームワークのバージョンを確認
- `aiplatform.autolog()` が `model.fit()` の前に呼ばれているか確認
- Keras 3.0 以降は非サポート

## 関連ドキュメント

- [Vertex AI Experiments ドキュメント](https://cloud.google.com/vertex-ai/docs/experiments)
- [autolog() リファレンス](https://cloud.google.com/vertex-ai/docs/experiments/autolog-data)
- [Pipelines との連携](https://cloud.google.com/vertex-ai/docs/experiments/integrate-with-pipelines)
