import logging
from typing import List
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..config import Config
from ..models import InferenceResult


class BigQueryRepository:
    """BigQuery データベースへのアクセスを管理するリポジトリ"""

    def __init__(self, config: Config):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        self.table_ref = f"{config.project_id}.{config.dataset_id}.{config.table_id}"
        self.logger = logging.getLogger(self.__class__.__name__)

    def ensure_table_exists(self) -> None:
        """テーブルが存在することを確認し、存在しない場合は作成"""
        try:
            table_id = f"{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}"
            table = self.client.get_table(table_id)
            self.logger.info(f"テーブル {table_id} は既に存在します")
        except NotFound:
            self.logger.info(f"テーブル {table_id} が見つからないため作成します")
            self._create_table()

    def _create_table(self) -> None:
        """推論結果テーブルを作成"""
        dataset_ref = self.client.dataset(self.config.dataset_id, project=self.config.project_id)
        
        # データセットが存在しない場合は作成
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-northeast1"  # Tokyo region
            dataset = self.client.create_dataset(dataset)
            self.logger.info(f"データセット {self.config.dataset_id} を作成しました")

        # テーブルスキーマを定義
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("inference_value", "FLOAT", mode="REQUIRED"),
        ]

        table_ref = dataset_ref.table(self.config.table_id)
        table = bigquery.Table(table_ref, schema=schema)
        
        # パーティション設定（日付ベース）
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp"
        )
        
        table = self.client.create_table(table)
        self.logger.info(f"テーブル {table.table_id} を作成しました")

    def insert_inference_result(self, result: InferenceResult) -> bool:
        """推論結果を1件挿入"""
        return self.insert_inference_results([result])

    def insert_inference_results(self, results: List[InferenceResult]) -> bool:
        """推論結果を複数件挿入"""
        if not results:
            self.logger.warning("挿入するデータがありません")
            return True

        try:
            # BigQueryに挿入するためのデータを準備
            rows_to_insert = [result.to_dict() for result in results]
            
            table = self.client.get_table(self.table_ref)
            errors = self.client.insert_rows_json(table, rows_to_insert)

            if errors:
                self.logger.error(f"BigQuery挿入エラー: {errors}")
                return False
            
            self.logger.info(f"{len(results)}件のデータを正常に挿入しました")
            return True

        except Exception as e:
            self.logger.error(f"BigQuery挿入処理中にエラーが発生: {str(e)}")
            return False