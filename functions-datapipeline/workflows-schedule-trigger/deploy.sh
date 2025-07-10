#!/bin/bash

# Cloud Workflows デプロイスクリプト
# 時系列データ取得処理の定期実行ワークフローをデプロイ

# 変数設定
PREFIX=""

# PREFIXバリデーション
if [ -z "$PREFIX" ]; then
    echo "エラー: PREFIX変数が空です。"
    echo "PREFIX変数にプロジェクト固有の識別子を設定してください。"
    echo "例: PREFIX=\"hayashi\""
    exit 1
fi

WORKFLOW_NAME="${PREFIX}-timeseries-data-workflow"
REGION="asia-northeast1"  # 東京リージョン
PROJECT_ID=$(gcloud config get-value project)
SERVICE_ACCOUNT_EMAIL="${PREFIX}-workflows-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Cloud Workflows デプロイ開始 ==="
echo "ワークフロー名: $WORKFLOW_NAME"
echo "リージョン: $REGION"
echo "プロジェクトID: $PROJECT_ID"

# サービスアカウントの存在確認
echo "サービスアカウントの確認..."
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT_EMAIL --quiet 2>/dev/null; then
    echo "サービスアカウントが存在しません。作成します..."
    gcloud iam service-accounts create ${PREFIX}-workflows-sa \
        --display-name="Workflows Service Account" \
        --description="Service account for Cloud Workflows execution"
    
    # 必要な権限を付与
    echo "権限を付与中..."
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
        --role="roles/cloudfunctions.invoker"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
        --role="roles/logging.logWriter"
    
    echo "サービスアカウントの作成と権限付与が完了しました"
else
    echo "サービスアカウントが既に存在します"
fi

# ワークフローのデプロイ
echo "ワークフローをデプロイ中..."
gcloud workflows deploy $WORKFLOW_NAME \
    --source=workflow.yaml \
    --location=$REGION \
    --service-account=$SERVICE_ACCOUNT_EMAIL

if [ $? -eq 0 ]; then
    echo "=== デプロイ成功 ==="
    echo "ワークフロー情報:"
    gcloud workflows describe $WORKFLOW_NAME --location=$REGION
    
    echo ""
    echo "=== Cloud Scheduler の設定 ==="
    echo "定期実行を設定するには、以下のコマンドを実行してください:"
    echo ""
    echo "# 毎日午前9時に実行 (JST)"
    echo "gcloud scheduler jobs create http ${WORKFLOW_NAME}-schedule \\"
    echo "  --schedule=\"0 9 * * *\" \\"
    echo "  --time-zone=\"Asia/Tokyo\" \\"
    echo "  --uri=\"https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}/executions\" \\"
    echo "  --http-method=POST \\"
    echo "  --oauth-service-account-email=\"$SERVICE_ACCOUNT_EMAIL\" \\"
    echo "  --headers=\"Content-Type=application/json\" \\"
    echo "  --message-body='{}'"
    echo ""
    echo "# 手動実行テスト"
    echo "gcloud workflows execute $WORKFLOW_NAME --location=$REGION"
    
else
    echo "=== デプロイ失敗 ==="
    exit 1
fi