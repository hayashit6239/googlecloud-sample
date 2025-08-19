#!/bin/bash

# Cloud Run functions デプロイスクリプト

# 変数設定
PREFIX=""

# PREFIXバリデーション
if [ -z "$PREFIX" ]; then
    echo "エラー: PREFIX変数が空です。"
    echo "PREFIX変数にプロジェクト固有の識別子を設定してください。"
    echo "例: PREFIX=\"hayashi\""
    exit 1
fi

FUNCTION_NAME="${PREFIX}-bq-insert-inference"
REGION="asia-northeast1"  # 東京リージョン
RUNTIME="python311"
MEMORY="512MB"
TIMEOUT="540s"
PROJECT_ID=$(gcloud config get-value project)
SERVICE_ACCOUNT_NAME="${PREFIX}-functions-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Cloud Run function デプロイ開始 ==="
echo "関数名: $FUNCTION_NAME"
echo "リージョン: $REGION"
echo "ランタイム: $RUNTIME"
echo "サービスアカウント: $SERVICE_ACCOUNT_EMAIL"

# サービスアカウントの存在確認と作成
echo "サービスアカウントの確認..."
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT_EMAIL --quiet 2>/dev/null; then
    echo "サービスアカウントが存在しません。作成します..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
        --display-name="Cloud Functions Service Account" \
        --description="Service account for Cloud Functions execution"
    
    if [ $? -ne 0 ]; then
        echo "エラー: サービスアカウントの作成に失敗しました"
        exit 1
    fi
else
    echo "サービスアカウントが既に存在します"
fi

# 必要な権限を付与
echo "必要な権限を付与中..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/bigquery.dataEditor" \
    --quiet

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/bigquery.user" \
    --quiet

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/logging.logWriter" \
    --quiet

# デプロイ実行
gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=$RUNTIME \
  --region=$REGION \
  --source=. \
  --entry-point=insert_inference_result \
  --trigger-http \
  --allow-unauthenticated \
  --memory=$MEMORY \
  --timeout=$TIMEOUT \
  --service-account=$SERVICE_ACCOUNT_EMAIL \
  --env-vars-file=.local.env.yaml

if [ $? -eq 0 ]; then
    echo "=== デプロイ成功 ==="
    echo "関数URL:"
    gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(serviceConfig.uri)"
else
    echo "=== デプロイ失敗 ==="
    exit 1
fi