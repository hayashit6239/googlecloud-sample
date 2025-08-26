import logging
import random
from datetime import datetime
from typing import Dict, Any

from ..models import InferenceResult
from ..repositories.bigquery_repository import BigQueryRepository


class InferenceService:
    """推論処理とBigQuery挿入を管理するサービス"""

    def __init__(self, bigquery_repository: BigQueryRepository):
        self.nodeai_api_key = ""
        self.bigquery_repository = bigquery_repository
        self.logger = logging.getLogger(self.__class__.__name__)

    def process_inference(self) -> Dict[str, Any]:
        """推論処理を実行してBigQueryに挿入"""
        try:
            self.logger.info("推論処理を開始します")

            # テーブルの存在確認と作成
            self.bigquery_repository.ensure_table_exists()

            # ランダムな推論値を生成
            inference_value = self._generate_random_inference()
            current_time = datetime.now()

            # 推論結果オブジェクトを作成
            result = InferenceResult(
                timestamp=current_time, inference_value=inference_value
            )

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

    def _generate_random_inference(self) -> float:
        """ランダムな推論値を生成"""
        # 0.0から1.0の間のランダムな値を生成
        return random.uniform(0.0, 1.0)

    def _create_request_headers(self) -> Dict[str, str]:
        """NodeAI APIのリクエストヘッダー作成"""
        self.logger.info("NodeAI APIのリクエストヘッダー作成")

        try:
            headers = {
                "Authorization": f"Bearer {self.nodeai_api_key}",
                "Content-Type": "application/json",
            }
        except ValueError as e:
            self.logger.error(f"APIキーを認識できません: {e}")
            raise e

        self.logger.info("リクエストヘッダー作成完了")
        self.logger.debug(f"リクエストヘッダー: {headers}")

        return headers
