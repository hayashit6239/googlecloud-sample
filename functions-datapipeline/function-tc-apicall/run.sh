#!/bin/bash

# ローカル実行スクリプト

echo "=== Cloud Functions ローカル実行 ==="

# 環境変数を設定
export TENANT_DOMAIN=""
export AUTHORIZATION=""
export SOURCE=""

# Functions Framework でローカル実行
echo "ローカルサーバーを起動中..."
echo "URL: http://localhost:8080"
echo "停止するには Ctrl+C を押してください"

functions-framework --target=fetch_timeseries_data --debug