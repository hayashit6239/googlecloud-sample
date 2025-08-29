"""
NodeAI APIとの通信を担当するリポジトリ
"""
import requests
import logging
import base64
import json
import datetime
from typing import Dict, Any

from ..schemas import NodeAIApiResponse, AnomalyResult, ThresholdValues
from ..config import Config


class NodeaiRepository:
    """NodeAI API リポジトリ"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _create_request_headers(self) -> Dict[str, str]:
        """ NodeAI APIのリクエストヘッダー作成"""
        self.logger.info("NodeAI APIのリクエストヘッダー作成")
            
        try:
            headers = {
                "Authorization": f"Bearer {self.config.nodeai_api_key}",
                "Content-Type": "application/json",
            }
        except ValueError as e:
            self.logger.error(f"APIキーを認識できません: {e}")
            raise e

        self.logger.info("リクエストヘッダー作成完了")
        self.logger.debug(f"リクエストヘッダー: {headers}")

        return headers
    
    def _create_payload(self) -> Dict[str, Any]:
        """ NodeAI APIのペイロード作成"""
        self.logger.info("NodeAI APIのペイロード作成")
        self.logger.info(f"読み込み対象のローカルファイルパス: {self.config.csv_file_path}")
        
        try:
            with open(self.config.csv_file_path, "rb") as f:
                data = f.read()
                encoded_data = base64.b64encode(data).decode('utf-8')

        except (FileNotFoundError, IOError) as e:
            self.logger.error(f"ファイルの読み込み中にエラーが発生しました: {e}")
            raise e

        self.logger.info("データのbase64エンコード完了")

        payload = {"inferenceDataset": encoded_data}
        self.logger.info("ペイロード作成完了")
        self.logger.debug(f"ペイロード: {payload}")

        return payload
    
    def send_inference_request(self) -> NodeAIApiResponse:
        """ NodeAI APIにリクエストを送信し、レスポンスを取得"""
        headers = self._create_request_headers()
        payload = self._create_payload()
        self.logger.info("NodeAI APIにリクエスト送信")
        url = f"{self.config.nodeai_base_url}/v1/anomalyDetection/mlp/inference/input/csv/{self.config.nodeai_api_id}"
        self.logger.info(f"リクエストURL: {url}")

        try:
            start_time = datetime.datetime.now()
            response = requests.post(
                url=url,
                headers=headers,
                json=payload
            )
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"APIリクエスト成功 - ステータス: {response.status_code}, 応答時間: {elapsed_time:.2f}秒"
            )
            self.logger.info(f"レスポンスサイズ: {len(response.content)} bytes")
            self.logger.debug(f"response content: {response.content}")

            response_data = response.json()
                    
            parse_data = self.parse_api_response(response_data)
            return parse_data
        
        except requests.Timeout as e:
            self.logger.error(f"APIリクエストがタイムアウトしました: {str(e)}")
            raise Exception(f"APIリクエストがタイムアウトしました: {str(e)}")
        except requests.RequestException as e:
            self.logger.error(f"APIリクエストエラー - {type(e).__name__}: {str(e)}")
            raise Exception(f"APIからのデータ取得に失敗しました: {str(e)}")
        except json.JSONDecodeError as e:
            self.logger.error(f"JSONデコードエラー - 位置 {e.pos}: {e.msg}")
            raise ValueError("APIレスポンスのJSON形式が正しくありません")
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"nodeaiの推論でエラーが発生しました")
            self.logger.warning(f"Nullの推論値を生成します")
            parse_data = self.create_null_api_response()
            return parse_data
    
    def parse_api_response(self, response_data: Dict[str, Any]) -> NodeAIApiResponse:
        """NodeAI APIレスポンスをパース"""
        try:
            # results配列をパース
            results = [
                AnomalyResult(
                    timestamp=item["timestamp"],
                    reconstructionError=float(item["reconstructionError"])
                )
                for item in response_data["results"]
            ]
            
            # threshold情報をパース
            threshold_data = response_data["threshold"]
            threshold = ThresholdValues(
                sigma2=float(threshold_data["2sigma"]),
                sigma3=float(threshold_data["3sigma"])
            )
            
            return NodeAIApiResponse(results=results, threshold=threshold)
            
        except KeyError as e:
            self.logger.error(f"APIレスポンスのパースに失敗（KeyError）: {e}")
            self.logger.error(f"受信データ: {response_data}")
            raise KeyError(f"APIレスポンスに必要なキーがありません: {e}")
        except ValueError as e:
            self.logger.error(f"APIレスポンスのパースに失敗（ValueError）: {e}")
            self.logger.error(f"受信データ: {response_data}")
            raise ValueError(f"APIレスポンスの値が不正です: {e}")
        except TypeError as e:
            self.logger.error(f"APIレスポンスのパースに失敗（TypeError）: {e}")
            self.logger.error(f"受信データ: {response_data}")
            raise TypeError(f"APIレスポンスの型が不正です: {e}")
    
    def create_null_api_response(self) -> NodeAIApiResponse:
        """ nodeaiの予測値を生成 """
        timestamp = (datetime.datetime.now() - datetime.timedelta(minutes=self.config.tc_data_delay_minutes)).replace(second=44, microsecond=0)
        result = [
            AnomalyResult(
                timestamp=str(timestamp),
                reconstructionError=None
            )
        ]
        threshold = ThresholdValues(
                sigma2=float(self.config.threshold),
                sigma3=float(self.config.threshold)
            )
        return NodeAIApiResponse(results=result, threshold=threshold)