# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## デプロイコマンド

```
gcloud beta run worker-pools deploy xxx --region=asia-northeast1 --service-account=xxx --image=asia-northeast1-docker.pkg.dev/xxx --set-env-vars=GOOGLE_CLOUD_PROJECT=xxx,PUBSUB_SUBSCRIPTION_ID=xxx,STORAGE_BUCKET_NAME=xxx
```

## アーキテクチャ

### プロジェクト構成

- `workerpools-pubsub/`: メインアプリケーション（Cloud Run worker pools 用）
  - `cmd/worker/main.go`: エントリーポイント
  - `internal/pubsub/`: Pub/Sub 購読機能
  - `internal/storage/`: Cloud Storage ダウンロード機能
  - `docs/仕様書.md`: システム仕様

### システム概要

Cloud Run worker pools で動作する Pull 型サブスクライバー：

1. Cloud Pub/Sub からメッセージを受信（path キーで Cloud Storage パス指定）
2. Cloud Storage から指定パスのファイル群をダウンロード
3. ダウンロード完了後、ファイル一覧をログ出力

### 主要コンポーネント

- `pubsub.Subscriber`: Pub/Sub メッセージ購読と JSON 解析
- `storage.Downloader`: Cloud Storage ディレクトリ一括ダウンロード
- メッセージハンドラー: ダウンロード処理とファイル一覧出力の統合

### 環境変数

- `GOOGLE_CLOUD_PROJECT`: Google Cloud プロジェクト ID
- `PUBSUB_SUBSCRIPTION_ID`: Pub/Sub サブスクリプション ID
- `STORAGE_BUCKET_NAME`: Cloud Storage バケット名
- `DOWNLOAD_DIR`: ダウンロード先ディレクトリ（デフォルト: /tmp/downloads）
