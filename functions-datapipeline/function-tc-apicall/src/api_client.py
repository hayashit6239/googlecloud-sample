import datetime
import requests
import json
import logging
from typing import Dict, List, Optional, Any, Tuple

from .config import Config
from .models import MeasurementPoint, SensorSchema


class TimeSeriesAPIClient:
    """時系列データAPI クライアント"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def fetch_data(
        self,
    ) -> Tuple[List[SensorSchema], Dict[str, List[Optional[MeasurementPoint]]]]:
        """APIから時系列データとスキーマを取得"""
        url = self._build_api_url()
        headers = {"Authorization": f"Basic {self.config.authorization}"}

        self.logger.info(f"API呼び出し開始 - URL: {url}")
        self.logger.debug(f"リクエストヘッダー: Authorization=Basic [MASKED]")

        try:
            start_time = datetime.datetime.now()
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"APIリクエスト成功 - ステータス: {response.status_code}, 応答時間: {elapsed_time:.2f}秒"
            )
            self.logger.debug(f"レスポンスサイズ: {len(response.content)} bytes")

            data = response.json()
            return self._parse_response(data)

        except requests.Timeout as e:
            self.logger.error(f"APIリクエストがタイムアウトしました: {str(e)}")
            raise Exception(f"APIリクエストがタイムアウトしました: {str(e)}")
        except requests.RequestException as e:
            self.logger.error(f"APIリクエストエラー - {type(e).__name__}: {str(e)}")
            raise Exception(f"APIからのデータ取得に失敗しました: {str(e)}")
        except json.JSONDecodeError as e:
            self.logger.error(f"JSONデコードエラー - 位置 {e.pos}: {e.msg}")
            raise ValueError("APIレスポンスのJSON形式が正しくありません")

    def _build_api_url(self) -> str:
        """API URLを構築"""
        now = datetime.datetime.now()
        date_to = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        date_from = (now - datetime.timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        url = (
            f"https://{self.config.tenant_domain}/measurement/measurements/series"
            f"?source={self.config.source}&dateFrom={date_from}&dateTo={date_to}"
        )

        self.logger.debug(
            f"構築されたAPI URL - テナント: {self.config.tenant_domain}, ソース: {self.config.source}"
        )
        self.logger.debug(f"取得期間: {date_from} ～ {date_to}")

        return url

    def _parse_response(
        self, data: Dict[str, Any]
    ) -> Tuple[List[SensorSchema], Dict[str, List[Optional[MeasurementPoint]]]]:
        """APIレスポンスを解析"""
        self.logger.debug("APIレスポンスの解析を開始")

        # スキーマ情報を取得
        series_data = data.get("series", [])
        schemas = [
            SensorSchema(name=series["name"], unit=series["unit"], type=series["type"])
            for series in series_data
        ]

        self.logger.info(f"センサースキーマを解析 - センサー数: {len(schemas)}")
        for i, schema in enumerate(schemas[:5]):  # 最初の5個だけログ出力
            self.logger.debug(f"センサー{i+1}: {schema.name} ({schema.type})")
        if len(schemas) > 5:
            self.logger.debug(f"... 他 {len(schemas) - 5} 個のセンサー")

        # 時系列データを取得
        values = data.get("values", {})
        timeseries_data = {}
        measurement_count = 0

        for timestamp, measurements in values.items():
            timeseries_data[timestamp] = [
                MeasurementPoint.from_dict(measurement) for measurement in measurements
            ]
            measurement_count += len([m for m in measurements if m is not None])

        self.logger.info(
            f"時系列データ解析完了 - タイムスタンプ数: {len(timeseries_data)}, 計測値数: {measurement_count}"
        )

        if timeseries_data:
            first_timestamp = list(timeseries_data.keys())[0]
            last_timestamp = list(timeseries_data.keys())[-1]
            self.logger.debug(f"データ期間: {first_timestamp} ～ {last_timestamp}")

        return schemas, timeseries_data