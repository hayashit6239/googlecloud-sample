import functions_framework
import datetime
import json
import logging

from src.config import Config
from src.repositories.bigquery_repository import BigQueryRepository
from src.services.inference_service import InferenceService


# ログ設定 - Cloud Runの標準出力対応
def setup_logging():
    """Cloud Run用のログ設定"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # 既存のハンドラーをクリア
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 標準出力ハンドラーを追加
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    
    # フォーマッター設定
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
    stream_handler.setFormatter(formatter)
    
    # ルートロガーにハンドラーを追加
    root_logger.addHandler(stream_handler)
    
    # 外部ライブラリのログレベルを調整
    logging.getLogger("google.cloud.bigquery").setLevel(logging.WARNING)

# ログ設定を実行
setup_logging()


# Cloud Functions Entry Point
@functions_framework.http
def insert_inference_result(request):
    """推論結果をBigQueryに挿入するCloud Function"""
    logger = logging.getLogger(f"{__name__}.insert_inference_result")
    request_start_time = datetime.datetime.now()

    # リクエスト情報をログ出力
    logger.info("========== Cloud Run function 開始 ==========")
    logger.info(f"リクエストメソッド: {request.method}")
    logger.info(f"URL: {request.url}")
    logger.info(f"ヘッダー: {dict(request.headers)}")

    # CORS対応
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
        return ("", 204, headers)

    headers = {
        "Access-Control-Allow-Origin": "*",
        "Content-Type": "application/json; charset=utf-8",
    }

    try:
        # 設定読み込みと検証
        logger.info("設定読み込み中...")
        config = Config.from_environment()
        
        # 設定検証
        validation_error = _validate_config(config)
        if validation_error:
            return _create_error_response(validation_error, 400, headers, request_start_time, logger)
        
        logger.info("設定検証完了")

        # 依存関係の構築
        bigquery_repository = BigQueryRepository(config)
        
        # サービスの作成
        inference_service = InferenceService(bigquery_repository=bigquery_repository)

        # 推論処理とBigQuery挿入
        logger.info("推論処理とBigQuery挿入を開始")
        result = inference_service.process_inference()

        # 成功レスポンス
        request_elapsed = (datetime.datetime.now() - request_start_time).total_seconds()
        logger.info(f"========== Cloud Function 成功 ==========")
        logger.info(f"総リクエスト時間: {request_elapsed:.2f}秒")

        # レスポンスデータを構築
        response_data = {
            "message": "推論結果のBigQuery挿入が完了しました",
            "status": "success",
            "processing_time_seconds": round(request_elapsed, 2),
            **result
        }

        return (json.dumps(response_data, ensure_ascii=False), 200, headers)

    except ValueError as e:
        # 設定エラー（400 Bad Request）
        return _create_error_response(
            f"設定エラー: {str(e)}", 
            400, 
            headers, 
            request_start_time, 
            logger,
            "configuration_error"
        )

    except Exception as e:
        # その他のエラー（500 Internal Server Error）
        return _create_error_response(
            f"内部エラーが発生しました: {str(e)}", 
            500, 
            headers, 
            request_start_time, 
            logger,
            "internal_error"
        )


def _validate_config(config: Config) -> str:
    """設定の検証"""
    try:
        config.validate()
        return None
    except ValueError as e:
        return str(e)


def _create_error_response(message: str, status_code: int, headers: dict, request_start_time: datetime.datetime, logger, error_type: str = "error"):
    """エラーレスポンスを作成"""
    request_elapsed = (datetime.datetime.now() - request_start_time).total_seconds()
    
    log_level = "error" if status_code >= 500 else "warning"
    log_message = f"========== Cloud Function エラー =========="
    
    getattr(logger, log_level)(log_message)
    getattr(logger, log_level)(f"エラー: {message}")
    getattr(logger, log_level)(f"リクエスト時間: {request_elapsed:.2f}秒")

    error_response = {
        "message": message,
        "status": "error",
        "error_type": error_type,
    }
    
    return (json.dumps(error_response, ensure_ascii=False), status_code, headers)