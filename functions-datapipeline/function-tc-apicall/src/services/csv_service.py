import csv
import logging
import os
import tempfile
from typing import Dict, List, Optional

from ..models import SensorSchema, MeasurementPoint


class CSVService:
    """CSV処理を担当するサービス"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create_csv_from_timeseries(
        self, 
        schemas: List[SensorSchema], 
        timeseries_data: Dict[str, List[Optional[MeasurementPoint]]]
    ) -> str:
        """
        時系列データからCSVファイルを作成
        
        Args:
            schemas: センサーのスキーマ情報
            timeseries_data: 時系列データ（timestamp -> measurements）
            
        Returns:
            作成されたCSVファイルのパス
        """
        self.logger.info("CSV作成開始")
        
        # 一時ファイルを作成
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        temp_file_path = temp_file.name
        
        try:
            # CSVヘッダーを準備
            headers = ['timestamp']
            for schema in schemas:
                headers.extend([f"{schema.name}_min", f"{schema.name}_max"])
            
            # CSVライターを初期化
            writer = csv.writer(temp_file)
            writer.writerow(headers)
            
            # データを行ごとに書き込み
            for timestamp, measurements in timeseries_data.items():
                row = [timestamp]
                for measurement in measurements:
                    if measurement:
                        row.extend([measurement.min_value, measurement.max_value])
                    else:
                        row.extend([None, None])
                writer.writerow(row)
            
            temp_file.close()
            self.logger.info(f"CSV作成完了: {temp_file_path}")
            return temp_file_path
            
        except Exception as e:
            temp_file.close()
            # エラー時にファイルを削除
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            raise e

    def get_file_size(self, file_path: str) -> int:
        """
        ファイルサイズを取得
        
        Args:
            file_path: ファイルパス
            
        Returns:
            ファイルサイズ（バイト）
        """
        return os.path.getsize(file_path)

    def cleanup_temp_file(self, file_path: str) -> None:
        """
        一時ファイルを削除
        
        Args:
            file_path: 削除するファイルパス
        """
        if file_path and os.path.exists(file_path):
            os.unlink(file_path)
            self.logger.info(f"一時ファイルを削除しました: {file_path}")

    def validate_csv_data(
        self, 
        schemas: List[SensorSchema], 
        timeseries_data: Dict[str, List[Optional[MeasurementPoint]]]
    ) -> bool:
        """
        CSV作成前のデータ検証
        
        Args:
            schemas: センサーのスキーマ情報
            timeseries_data: 時系列データ
            
        Returns:
            検証結果（True: 正常、False: 異常）
        """
        if not schemas:
            self.logger.warning("スキーマ情報が空です")
            return False
        
        if not timeseries_data:
            self.logger.warning("時系列データが空です")
            return False
        
        # 各タイムスタンプのメジャーメント数がスキーマ数と一致するかチェック
        for timestamp, measurements in timeseries_data.items():
            if len(measurements) != len(schemas):
                self.logger.warning(
                    f"タイムスタンプ {timestamp} のメジャーメント数 ({len(measurements)}) "
                    f"がスキーマ数 ({len(schemas)}) と一致しません"
                )
                return False
        
        self.logger.info("CSV データ検証完了")
        return True