import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from ..config import Config
from ..models import SensorSchema, MeasurementPoint
from ..repositories.time_series_repository import TimeSeriesRepository
from ..repositories.storage_repository import StorageRepository
from .csv_service import CSVService


class TimeSeriesService:
    """時系列データ処理のメインビジネスロジック"""

    def __init__(
        self,
        time_series_repository: TimeSeriesRepository,
        storage_repository: Optional[StorageRepository] = None,
        csv_service: Optional[CSVService] = None,
    ):
        self.time_series_repository = time_series_repository
        self.storage_repository = storage_repository
        self.csv_service = csv_service or CSVService()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def process_time_series_data(self) -> Dict:
        """
        時系列データを取得・処理・保存する
        
        Returns:
            処理結果の辞書
        """
        self.logger.info("時系列データ処理開始")
        
        try:
            # 時系列データを取得
            schemas, timeseries_data = self.time_series_repository.fetch_time_series_data()
            
            # 基本の処理結果を作成
            result = {
                "data_summary": {
                    "sensor_count": len(schemas),
                    "timestamp_count": len(timeseries_data),
                    "sensors": [
                        {"name": schema.name, "unit": schema.unit, "type": schema.type}
                        for schema in schemas[:10]  # 最初の10個のセンサー情報
                    ],
                }
            }
            
            # サンプルデータを追加
            if timeseries_data:
                first_timestamp = list(timeseries_data.keys())[0]
                sample_measurements = timeseries_data[first_timestamp]
                
                result["sample_data"] = {
                    "timestamp": first_timestamp,
                    "measurements": [
                        {
                            "min": measurement.min_value if measurement else None,
                            "max": measurement.max_value if measurement else None,
                        }
                        for measurement in sample_measurements[:5]  # 最初の5個の計測値
                    ],
                }
            
            # CSV保存処理
            csv_result = self._process_csv_storage(schemas, timeseries_data)
            if csv_result:
                result["csv_storage"] = csv_result
            
            self.logger.info("時系列データ処理完了")
            return result
            
        except Exception as e:
            self.logger.error(f"時系列データ処理エラー: {e}")
            raise e

    def _process_csv_storage(
        self, 
        schemas: List[SensorSchema], 
        timeseries_data: Dict[str, List[Optional[MeasurementPoint]]]
    ) -> Optional[Dict]:
        """
        CSVファイル作成とストレージ保存を処理
        
        Args:
            schemas: センサーのスキーマ情報
            timeseries_data: 時系列データ
            
        Returns:
            CSV処理結果の辞書（保存しない場合はNone）
        """
        if not self.storage_repository:
            self.logger.info("ストレージリポジトリが設定されていないため、CSV保存をスキップします")
            return None
        
        temp_file_path = None
        
        try:
            # データ検証
            if not self.csv_service.validate_csv_data(schemas, timeseries_data):
                return {
                    "success": False,
                    "error": "CSV データの検証に失敗しました"
                }
            
            # CSVファイルを作成
            temp_file_path = self.csv_service.create_csv_from_timeseries(schemas, timeseries_data)
            
            # ストレージ用のファイル名を生成
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            destination_path = f"timeseries_data/{timestamp}.csv"
            
            # ストレージにアップロード
            file_url = self.storage_repository.upload_file(temp_file_path, destination_path)
            
            # ファイルサイズを取得
            file_size = self.csv_service.get_file_size(temp_file_path)
            
            return {
                "success": True,
                "file_url": file_url,
                "file_size_bytes": file_size,
                "destination_path": destination_path,
                "timestamp": timestamp
            }
            
        except Exception as e:
            self.logger.error(f"CSV処理・格納エラー: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
        finally:
            # 一時ファイルを削除
            if temp_file_path:
                self.csv_service.cleanup_temp_file(temp_file_path)

    def get_config_validation_error(self, config: Config) -> Optional[str]:
        """
        設定の検証エラーを取得
        
        Args:
            config: 設定オブジェクト
            
        Returns:
            エラーメッセージ（正常時はNone）
        """
        try:
            config.validate()
            return None
        except ValueError as e:
            return str(e)