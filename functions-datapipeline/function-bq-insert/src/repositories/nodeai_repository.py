"""
NodeAI APIとの通信を担当するリポジトリ
"""

import requests
import logging
import base64
import json
import datetime
import random
from typing import Dict, Any

from ..schemas import NodeAIApiResponse, AnomalyResult, ThresholdValues
from ..config import Config


class NodeaiRepository:
    """NodeAI API リポジトリ"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    # =====================
    # パブリックメソッド
    # =====================

    def get_inference_value(self) -> float:
        """NodeAI APIから推論値を取得するメインメソッド"""
        try:
            self.logger.info("NodeAI APIから推論値を取得します")
            api_response = self._send_inference_request()
            return self._extract_inference_value_from_response(api_response)
        except Exception as e:
            self.logger.error(f"NodeAI API呼び出し中にエラーが発生: {str(e)}")
            return self._generate_fallback_inference()

    # =====================
    # プライベートメソッド - API通信
    # =====================

    def _send_inference_request(self) -> NodeAIApiResponse:
        """NodeAI APIにリクエストを送信し、レスポンスを取得"""
        headers = self._create_request_headers()
        payload = self._create_payload()

        self.logger.info("NodeAI APIにリクエスト送信")
        url = self._build_api_url()
        self.logger.info(f"リクエストURL: {url}")

        try:
            response = self._execute_http_request(url, headers, payload)
            response_data = response.json()
            return self._parse_api_response(response_data)

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
            return self._create_null_api_response()

    def _execute_http_request(
        self, url: str, headers: Dict[str, str], payload: Dict[str, Any]
    ) -> requests.Response:
        """HTTP リクエストを実行"""
        jst = datetime.timezone(datetime.timedelta(hours=9))
        start_time = datetime.datetime.now(jst)
        response = requests.post(url=url, headers=headers, json=payload)
        elapsed_time = (datetime.datetime.now(jst) - start_time).total_seconds()

        self.logger.info(
            f"APIリクエスト成功 - ステータス: {response.status_code}, 応答時間: {elapsed_time:.2f}秒"
        )
        self.logger.info(f"レスポンスサイズ: {len(response.content)} bytes")
        self.logger.debug(f"response content: {response.content}")

        return response

    def _build_api_url(self) -> str:
        """API URLを構築"""
        return f"{self.config.nodeai_base_url}/v1/anomalyDetection/mlp/inference/input/csv/{self.config.nodeai_api_id}"

    # =====================
    # プライベートメソッド - リクエスト作成
    # =====================

    def _create_request_headers(self) -> Dict[str, str]:
        """NodeAI APIのリクエストヘッダー作成"""
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
        """NodeAI APIのペイロード作成"""
        self.logger.info("NodeAI APIのペイロード作成")
        self.logger.info(
            f"読み込み対象のローカルファイルパス: {self.config.csv_file_path}"
        )

        encoded_data = self._load_and_encode_csv_file()

        payload = {"inferenceDataset": encoded_data}
        self.logger.info("ペイロード作成完了")
        self.logger.debug(f"ペイロード: {payload}")
        return payload

    def _load_and_encode_csv_file(self) -> str:
        """CSVファイルを読み込み、base64エンコード"""
        try:
            with open(self.config.csv_file_path, "rb") as f:
                data = f.read()
                encoded_data = base64.b64encode(data).decode("utf-8")

            self.logger.info("データのbase64エンコード完了")
            return encoded_data

        except (FileNotFoundError, IOError) as e:
            self.logger.error(f"ファイルの読み込み中にエラーが発生しました: {e}")
            raise e

    # =====================
    # プライベートメソッド - レスポンス処理
    # =====================

    def _parse_api_response(self, response_data: Dict[str, Any]) -> NodeAIApiResponse:
        """NodeAI APIレスポンスをパース"""
        try:
            results = self._parse_results(response_data["results"])
            threshold = self._parse_threshold(response_data["threshold"])
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

    def _parse_results(self, results_data: list) -> list[AnomalyResult]:
        """results配列をパース"""
        return [
            AnomalyResult(
                timestamp=item["timestamp"],
                reconstructionError=float(item["reconstructionError"]),
            )
            for item in results_data
        ]

    def _parse_threshold(self, threshold_data: Dict[str, Any]) -> ThresholdValues:
        """threshold情報をパース"""
        return ThresholdValues(
            sigma2=float(threshold_data["2sigma"]),
            sigma3=float(threshold_data["3sigma"]),
        )

    def _extract_inference_value_from_response(
        self, api_response: NodeAIApiResponse
    ) -> float:
        """レスポンスから推論値を抽出"""
        if not api_response.results or len(api_response.results) == 0:
            self.logger.warning("NodeAI APIから結果が返されませんでした")
            return self._generate_fallback_inference()

        # 最新の結果を取得
        latest_result = api_response.results[-1]
        inference_value = latest_result.reconstructionError

        if inference_value is not None:
            self.logger.info(
                f"NodeAI APIから推論値を取得しました: {inference_value:.4f}"
            )
            return float(inference_value)
        else:
            self.logger.warning("NodeAI APIから有効な推論値が取得できませんでした")
            return self._generate_fallback_inference()

    # =====================
    # プライベートメソッド - フォールバック処理
    # =====================

    def _create_null_api_response(self) -> NodeAIApiResponse:
        """nodeaiのnull推論レスポンスを生成"""
        # JST（UTC+9）タイムゾーンでタイムスタンプを生成
        jst = datetime.timezone(datetime.timedelta(hours=9))
        timestamp = (
            datetime.datetime.now(jst)
            - datetime.timedelta(minutes=self.config.tc_data_delay_minutes)
        ).replace(second=44, microsecond=0)

        result = [AnomalyResult(timestamp=str(timestamp), reconstructionError=None)]
        threshold = ThresholdValues(
            sigma2=float(self.config.threshold), sigma3=float(self.config.threshold)
        )
        return NodeAIApiResponse(results=result, threshold=threshold)

    def _generate_fallback_inference(self) -> float:
        """フォールバック用のランダム推論値を生成"""
        fallback_value = random.uniform(0.0, 1.0)
        self.logger.info(f"フォールバック値を生成しました: {fallback_value:.4f}")
        return fallback_value
