# BigQuery 推論結果挿入 Cloud Function

このCloud Functionは、実行されるたびにランダムな推論値を生成してBigQueryの指定テーブルに挿入します。

## 機能

- ランダムな推論値（0.0～1.0）の生成
- 現在のタイムスタンプとともにBigQueryに格納
- テーブルが存在しない場合の自動作成
- 適切なログ出力とエラーハンドリング

## 環境変数

| 変数名 | 説明 | 例 |
|--------|------|---|
| PROJECT_ID | Google Cloud プロジェクトID | `nodeai-20241029-hayashi-infr` |
| DATASET_ID | BigQuery データセットID | `inference_results` |
| TABLE_ID | BigQuery テーブルID | `daily_inferences` |

## BigQuery テーブル構造

```sql
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` (
  timestamp TIMESTAMP NOT NULL,
  inference_value FLOAT NOT NULL
)
PARTITION BY DATE(timestamp);
```

## ローカル実行

```bash
./run_local.sh
```

ローカル実行後、以下のURLにアクセスして動作確認：
```
curl http://localhost:8080
```

## デプロイ

1. PREFIX変数を設定してからデプロイスクリプトを実行：
```bash
# deploy.sh の PREFIX 変数を設定
PREFIX="hayashi"  # 自分の識別子に変更

./deploy.sh
```

## レスポンス例

成功時：
```json
{
  "message": "推論結果のBigQuery挿入が完了しました",
  "status": "success",
  "processing_time_seconds": 1.23,
  "inference_value": 0.7421,
  "timestamp": "2024-08-19T12:34:56.789",
  "bigquery_insert": "success"
}
```

エラー時：
```json
{
  "message": "設定エラー: PROJECT_ID環境変数が設定されていません",
  "status": "error",
  "error_type": "configuration_error"
}
```