import functions_framework
import datetime
import json
import logging

from src.config import Config
from src.api_client import TimeSeriesAPIClient


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
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

# ログ設定を実行
setup_logging()


# Cloud Functions Entry Point
@functions_framework.http
def fetch_timeseries_data(request):
    """時系列データを取得するCloud Function"""
    logger = logging.getLogger(f"{__name__}.fetch_timeseries_data")
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
        config.validate()
        logger.info("設定検証完了")

        # API クライアント作成
        logger.info("時系列データ取得開始")
        api_client = TimeSeriesAPIClient(config)
        schemas, timeseries_data = api_client.fetch_data()

        # 成功レスポンス
        request_elapsed = (datetime.datetime.now() - request_start_time).total_seconds()
        logger.info(f"========== Cloud Function 成功 ==========")
        logger.info(f"総リクエスト時間: {request_elapsed:.2f}秒")

        # レスポンスデータを構築
        response_data = {
            "message": "時系列データの取得が完了しました",
            "status": "success",
            "processing_time_seconds": round(request_elapsed, 2),
            "data_summary": {
                "sensor_count": len(schemas),
                "timestamp_count": len(timeseries_data),
                "sensors": [
                    {"name": schema.name, "unit": schema.unit, "type": schema.type}
                    for schema in schemas[:10]  # 最初の10個のセンサー情報
                ],
            },
        }

        # データが存在する場合は、サンプルデータも含める
        if timeseries_data:
            first_timestamp = list(timeseries_data.keys())[0]
            sample_measurements = timeseries_data[first_timestamp]

            response_data["sample_data"] = {
                "timestamp": first_timestamp,
                "measurements": [
                    {
                        "min": measurement.min_value if measurement else None,
                        "max": measurement.max_value if measurement else None,
                    }
                    for measurement in sample_measurements[:5]  # 最初の5個の計測値
                ],
            }

        return (json.dumps(response_data, ensure_ascii=False), 200, headers)

    except ValueError as e:
        # 設定エラー（400 Bad Request）
        request_elapsed = (datetime.datetime.now() - request_start_time).total_seconds()
        logger.error(f"========== 設定エラー ==========")
        logger.error(f"設定検証失敗: {e}")
        logger.error(f"リクエスト時間: {request_elapsed:.2f}秒")

        error_response = {
            "message": f"設定エラー: {str(e)}",
            "status": "error",
            "error_type": "configuration_error",
        }
        return (json.dumps(error_response, ensure_ascii=False), 400, headers)

    except Exception as e:
        # その他のエラー（500 Internal Server Error）
        request_elapsed = (datetime.datetime.now() - request_start_time).total_seconds()
        logger.error(f"========== Cloud Function エラー ==========")
        logger.error(f"予期しないエラー: {type(e).__name__}: {e}")
        logger.error(f"リクエスト時間: {request_elapsed:.2f}秒")

        error_response = {
            "message": f"内部エラーが発生しました: {str(e)}",
            "status": "error",
            "error_type": "internal_error",
        }
        return (json.dumps(error_response, ensure_ascii=False), 500, headers)
