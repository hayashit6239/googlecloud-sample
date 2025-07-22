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
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
        --role="roles/run.invoker"
    
    echo "サービスアカウントの作成と権限付与が完了しました"
else
    echo "サービスアカウントが既に存在します"
fi

# workflow.yaml内のprefix値をチェック
echo "workflow.yaml内のprefix設定を確認中..."
YAML_PREFIX=$(grep -E "^\s*-\s*prefix:\s*" workflow.yaml | sed -E 's/.*prefix:\s*"([^"]*)".*/\1/')

if [ -z "$YAML_PREFIX" ]; then
    echo "エラー: workflow.yaml内のprefix変数が空または見つかりません。"
    echo "workflow.yamlファイルの以下の行を修正してください:"
    echo "  - prefix: \"your-identifier\"  # 例: prefix: \"hayashi\""
    echo ""
    echo "現在の設定:"
    grep -n -E "^\s*-\s*prefix:" workflow.yaml || echo "  prefix設定が見つかりません"
    exit 1
fi

echo "workflow.yaml内prefix設定: $YAML_PREFIX"

# ワークフローのデプロイ
echo "ワークフローをデプロイ中..."
gcloud workflows deploy $WORKFLOW_NAME \
    --source=workflow.yaml \
    --location=$REGION \
    --service-account=$SERVICE_ACCOUNT_EMAIL

if [ $? -eq 0 ]; then
    echo "=== ワークフローデプロイ成功 ==="
    echo "ワークフロー情報:"
    gcloud workflows describe $WORKFLOW_NAME --location=$REGION
    
    echo ""
    echo "=== Cloud Scheduler の自動設定 ==="
    echo "毎日12:00に実行するスケジューラーを設定します..."
    
    # Cloud Scheduler の設定
    ./schedule.sh
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "=== 全体のデプロイ成功 ==="
        echo "ワークフローとスケジューラーの設定が完了しました。"
        echo ""
        echo "手動実行テスト:"
        echo "gcloud workflows execute $WORKFLOW_NAME --location=$REGION"
        echo ""
        echo "スケジューラー手動実行:"
        echo "gcloud scheduler jobs run ${WORKFLOW_NAME}-daily-schedule --location=$REGION"
    else
        echo ""
        echo "=== スケジューラー設定失敗 ==="
        echo "ワークフローのデプロイは成功しましたが、スケジューラーの設定に失敗しました。"
        echo "手動で ./schedule.sh を実行してください。"
    fi
    
else
    echo "=== デプロイ失敗 ==="
    exit 1
fi