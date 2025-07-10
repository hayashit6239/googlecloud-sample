# 時系列データ取得 Cloud Function

Azure Functions から Google Cloud Run Functions に移植した時系列データ取得サービスです。

## 機能

- TimeSeriesAPIClient による API 呼び出し
- 時系列データとセンサースキーマの取得
- 詳細なログ出力とエラーハンドリング
- CORS 対応

## ファイル構成

```
functions-datapipeline/
├── main.py              # メインの Cloud Function コード
├── requirements.txt     # Python依存関係
├── .env.yaml           # 環境変数設定（デプロイ用）
├── deploy.sh           # デプロイスクリプト
├── run_local.sh        # ローカル実行スクリプト
└── README.md           # このファイル
```

## ローカル開発

### 1. 依存関係のインストール

```bash
pip install -r requirements.txt
```

### 2. ローカル実行

```bash
./run_local.sh
```

### 3. テスト

```bash
curl http://localhost:8080
```

## デプロイ

### 1. gcloud CLI の設定

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 2. 環境変数の設定

`.env.yaml` ファイルを編集して、適切な値を設定してください。

### 3. デプロイ実行

```bash
./deploy.sh
```

## API レスポンス

### 成功時（200）

```json
{
  "message": "時系列データの取得が完了しました",
  "status": "success",
  "processing_time_seconds": 1.23,
  "data_summary": {
    "sensor_count": 26,
    "timestamp_count": 144,
    "sensors": [
      {
        "name": "rssi",
        "unit": "dBm",
        "type": "c8y_PeriodicNotification"
      }
    ]
  },
  "sample_data": {
    "timestamp": "2025-07-09T09:01:28.000Z",
    "measurements": [
      {
        "min": -63,
        "max": -63
      }
    ]
  }
}
```

### エラー時（400/500）

```json
{
  "message": "設定エラー: TENANT_DOMAIN環境変数が設定されていません",
  "status": "error",
  "error_type": "configuration_error"
}
```

## Azure Functions との主な違い

1. **エントリーポイント**: `@functions_framework.http` デコレータを使用
2. **リクエスト処理**: Flask-style の request オブジェクト
3. **レスポンス**: タプル形式での返却 `(body, status_code, headers)`
4. **環境変数**: `.env.yaml` ファイルでの管理
5. **CORS**: 手動で設定が必要

## ログ確認

```bash
# デプロイ後のログ確認
gcloud functions logs read fetch-timeseries-data --region=asia-northeast1
```

## トラブルシューティング

### デプロイエラー

- gcloud CLI が正しく設定されているか確認
- プロジェクト ID が正しく設定されているか確認
- 必要な API が有効化されているか確認

### 実行エラー

- 環境変数が正しく設定されているか確認
- ネットワーク接続に問題がないか確認
- ログでエラー詳細を確認
