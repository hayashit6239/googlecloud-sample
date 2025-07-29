#!/bin/bash

# Cloud Scheduler 設定スクリプト
# Workflows を毎日12:00に定期実行するスケジューラーを作成

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
SCHEDULER_JOB_NAME="${WORKFLOW_NAME}-daily-schedule"
REGION="asia-northeast1"  # 東京リージョン
PROJECT_ID=$(gcloud config get-value project)
SERVICE_ACCOUNT_EMAIL="${PREFIX}-workflows-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Cloud Scheduler 設定開始 ==="
echo "スケジューラージョブ名: $SCHEDULER_JOB_NAME"
echo "ワークフロー名: $WORKFLOW_NAME"
echo "実行時間: 毎日12:00 (JST)"
echo "リージョン: $REGION"
echo "プロジェクトID: $PROJECT_ID"

# ワークフローの存在確認
echo "ワークフローの存在確認..."
if ! gcloud workflows describe $WORKFLOW_NAME --location=$REGION --quiet 2>/dev/null; then
    echo "エラー: ワークフロー '$WORKFLOW_NAME' が存在しません。"
    echo "先にワークフローをデプロイしてください。"
    exit 1
fi

# サービスアカウントの存在確認
echo "サービスアカウントの確認..."
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT_EMAIL --quiet 2>/dev/null; then
    echo "エラー: サービスアカウント '$SERVICE_ACCOUNT_EMAIL' が存在しません。"
    echo "先にワークフローのデプロイでサービスアカウントを作成してください。"
    exit 1
fi

# サービスアカウントにWorkflows実行権限を付与
echo "サービスアカウントにWorkflows実行権限を付与中..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/workflows.invoker"

# 既存のスケジューラージョブの確認と削除
echo "既存のスケジューラージョブを確認中..."
if gcloud scheduler jobs describe $SCHEDULER_JOB_NAME --location=$REGION --quiet 2>/dev/null; then
    echo "既存のスケジューラージョブを削除します..."
    gcloud scheduler jobs delete $SCHEDULER_JOB_NAME --location=$REGION --quiet
fi

# Cloud Scheduler ジョブの作成
echo "Cloud Scheduler ジョブを作成中..."
gcloud scheduler jobs create http $SCHEDULER_JOB_NAME \
    --location=$REGION \
    --schedule="0 12 * * *" \
    --time-zone="Asia/Tokyo" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}/executions" \
    --http-method=POST \
    --oauth-service-account-email="$SERVICE_ACCOUNT_EMAIL" \
    --headers="Content-Type=application/json" \
    --message-body='{}'

if [ $? -eq 0 ]; then
    echo "=== Cloud Scheduler 設定成功 ==="
    echo ""
    echo "スケジューラージョブ情報:"
    gcloud scheduler jobs describe $SCHEDULER_JOB_NAME --location=$REGION
    
    echo ""
    echo "=== 手動テスト用コマンド ==="
    echo "# スケジューラージョブの手動実行:"
    echo "gcloud scheduler jobs run $SCHEDULER_JOB_NAME --location=$REGION"
    echo ""
    echo "# ワークフローの直接実行:"
    echo "gcloud workflows execute $WORKFLOW_NAME --location=$REGION"
    echo ""
    echo "# スケジューラージョブの削除 (必要に応じて):"
    echo "gcloud scheduler jobs delete $SCHEDULER_JOB_NAME --location=$REGION"
    
else
    echo "=== Cloud Scheduler 設定失敗 ==="
    exit 1
fi