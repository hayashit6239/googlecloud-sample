import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

from ..models import InferenceResult
from ..repositories.bigquery_repository import BigQueryRepository
from ..repositories.nodeai_repository import NodeaiRepository
from ..repositories.cloud_storage_repository import CloudStorageRepository
from ..schemas import NodeAIApiResponse


class InferenceService:
    """推論処理とBigQuery挿入を管理するサービス"""

    def __init__(
        self,
        bigquery_repository: BigQueryRepository,
        nodeai_repository: NodeaiRepository,
        cloud_storage_repository: CloudStorageRepository,
    ):
        self.bigquery_repository = bigquery_repository
        self.nodeai_repository = nodeai_repository
        self.cloud_storage_repository = cloud_storage_repository
        self.logger = logging.getLogger(self.__class__.__name__)

    def process_inference(self) -> Dict[str, Any]:
        """推論処理を実行してBigQueryに挿入"""
        try:
            self.logger.info("推論処理を開始します")

            # Cloud StorageからCSVファイルをダウンロード
            csv_ready = self.cloud_storage_repository.prepare_csv_file_for_inference(
                self.nodeai_repository.config.csv_file_path
            )
            if not csv_ready:
                raise Exception("CSVファイルの準備に失敗しました")

            # テーブルの存在確認と作成
            self.bigquery_repository.ensure_table_exists()

            # NodeAI APIで推論値を取得
            inference_value = self.nodeai_repository.get_inference_value()
            # JST（UTC+9）タイムゾーンで現在時刻を取得
            jst = timezone(timedelta(hours=9))
            current_time = datetime.now(jst)

            # 推論結果オブジェクトを作成
            result = InferenceResult(
                timestamp=current_time, inference_value=inference_value
            )
            self.logger.info(current_time)
            self.logger.info(f"推論結果を生成しました: {inference_value:.4f}")

            # BigQueryに挿入
            success = self.bigquery_repository.insert_inference_result(result)

            if success:
                self.logger.info("BigQueryへの挿入が完了しました")
                return {
                    "inference_value": inference_value,
                    "timestamp": current_time.isoformat(),
                    "bigquery_insert": "success",
                }
            else:
                self.logger.error("BigQueryへの挿入に失敗しました")
                return {
                    "inference_value": inference_value,
                    "timestamp": current_time.isoformat(),
                    "bigquery_insert": "failed",
                }

        except Exception as e:
            self.logger.error(f"推論処理中にエラーが発生: {str(e)}")
            raise
