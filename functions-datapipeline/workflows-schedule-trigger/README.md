# Cloud Workflows - 時系列データ取得スケジューラー

Cloud Run functions を定期的に実行するための Cloud Workflows です。

## 概要

このワークフローは以下の機能を提供します：

- 時系列データ取得 Cloud Run function (`fetch-timeseries-data`) の定期実行
- 失敗時の自動リトライ (最大 3 回)
- 詳細なログ出力
- エラーハンドリング

## ファイル構成

```
workflows-schedule-trigger/
├── workflow.yaml    # Cloud Workflowsの定義ファイル
├── deploy.sh       # デプロイスクリプト
└── README.md       # このファイル
```

## デプロイ方法

1. **前提条件**

   - Cloud Run functions (`fetch-timeseries-data`) が既にデプロイされていること
   - 必要な GCP の API が有効化されていること:
     - Cloud Workflows API
     - Cloud Scheduler API
     - Cloud Run functions API

2. **デプロイ実行**

   ```bash
   cd functions-datapipeline/workflows-schedule-trigger
   ./deploy.sh
   ```

3. **定期実行の設定**
   デプロイ後、以下のコマンドで Cloud Scheduler を設定します：
   ```bash
   # 毎日午前9時に実行 (JST)
   gcloud scheduler jobs create http hayashi-timeseries-data-workflow-schedule \
     --schedule="0 9 * * *" \
     --time-zone="Asia/Tokyo" \
     --uri="https://workflowexecutions.googleapis.com/v1/projects/YOUR_PROJECT_ID/locations/asia-northeast1/workflows/hayashi-timeseries-data-workflow/executions" \
     --http-method=POST \
     --oauth-service-account-email="workflows-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --headers="Content-Type=application/json" \
     --message-body='{}'
   ```

## 手動実行

ワークフローを手動で実行するには：

```bash
gcloud workflows execute hayashi-timeseries-data-workflow --location=asia-northeast1
```

## ワークフローの特徴

### セキュリティ（認証）

Cloud Run functions で「Require authentication」を有効にしている場合に対応：

- **OIDC認証**: HTTP呼び出しに `auth: type: OIDC` を設定
- **自動トークン取得**: ワークフロー実行時にサービスアカウントのIDトークン（OIDC JWT）を自動取得
- **認証ヘッダー追加**: HTTPリクエストに `Authorization: Bearer <ID_TOKEN>` ヘッダーを自動付与

**認証フロー:**
```
1. Workflows → GCP Identity Service: IDトークン取得
2. Workflows → Cloud Run functions: Authorization ヘッダー付きHTTP呼び出し
3. Cloud Run functions: トークンの署名・有効期限・発行者を検証
4. 認証成功 → 関数実行 / 認証失敗 → 403 Forbidden
```

**必要な権限:**
- `roles/cloudfunctions.invoker`: Cloud Functions 呼び出し権限
- `roles/run.invoker`: Cloud Run 呼び出し権限（Gen2 functions用）
- `roles/logging.logWriter`: ログ出力権限

### リトライ機能

- 最大 2 回までリトライを実行（初回 + リトライ1回）
- 失敗時は30秒待機後にリトライ

### ログ出力

- 実行開始/終了時のログ
- エラー発生時の詳細ログ
- 各試行の状況ログ

### エラーハンドリング

- HTTP エラーレスポンスの処理
- 通信エラーの処理
- 認証エラー（403 Forbidden）の処理
- 最大試行回数達成時の処理

## 監視

Cloud Logging でワークフローの実行状況を確認できます：

```bash
# ワークフローのログを確認
gcloud logging read "resource.type=workflow AND resource.labels.workflow_id=timeseries-data-workflow"
```

## スケジュール設定例

```bash
# 毎時実行
--schedule="0 * * * *"

# 毎日午前9時
--schedule="0 9 * * *"

# 平日の午前9時
--schedule="0 9 * * 1-5"

# 毎週月曜日の午前9時
--schedule="0 9 * * 1"
```

## トラブルシューティング

### よくある問題

1. **サービスアカウントの権限不足**

   - Cloud Run functions Invoker 権限が必要
   - Logging Writer 権限が必要

2. **Cloud Function URL の不一致**

   - `workflow.yaml`内の`function_name`を確認
   - 実際の Function 名と一致させる

3. **タイムアウトエラー**
   - Cloud Function の実行時間を確認
   - 必要に応じて`workflow.yaml`の`timeout`値を調整

### デバッグ方法

```bash
# ワークフローの実行履歴を確認
gcloud workflows executions list --workflow=timeseries-data-workflow --location=asia-northeast1

# 特定の実行の詳細を確認
gcloud workflows executions describe EXECUTION_ID --workflow=timeseries-data-workflow --location=asia-northeast1
```
