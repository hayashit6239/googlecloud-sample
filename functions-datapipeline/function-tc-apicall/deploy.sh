#!/bin/bash

# Cloud Functions デプロイスクリプト

# 変数設定
PREFIX="hayashi"

# PREFIXバリデーション
if [ -z "$PREFIX" ]; then
    echo "エラー: PREFIX変数が空です。"
    echo "PREFIX変数にプロジェクト固有の識別子を設定してください。"
    echo "例: PREFIX=\"hayashi\""
    exit 1
fi

FUNCTION_NAME="${PREFIX}-fetch-timeseries-data"
REGION="asia-northeast1"  # 東京リージョン
RUNTIME="python311"
MEMORY="512MB"
TIMEOUT="540s"

echo "=== Cloud Function デプロイ開始 ==="
echo "関数名: $FUNCTION_NAME"
echo "リージョン: $REGION"
echo "ランタイム: $RUNTIME"

# デプロイ実行
gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=$RUNTIME \
  --region=$REGION \
  --source=. \
  --entry-point=fetch_timeseries_data \
  --trigger-http \
  --allow-unauthenticated \
  --memory=$MEMORY \
  --timeout=$TIMEOUT \
  --env-vars-file=.local.env.yaml

if [ $? -eq 0 ]; then
    echo "=== デプロイ成功 ==="
    echo "関数URL:"
    gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(serviceConfig.uri)"
else
    echo "=== デプロイ失敗 ==="
    exit 1
fi