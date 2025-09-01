"""
Cloud Storageとの通信を担当するリポジトリ
"""
import logging
import tempfile
import os
from typing import Optional, Dict, Any
from google.cloud import storage
from google.cloud.exceptions import NotFound, Forbidden

from ..config import Config


class CloudStorageRepository:
    """Cloud Storage リポジトリ"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.client = storage.Client(project=self.config.project_id)
        self._resolved_file_name = None  # 解決されたファイル名をキャッシュ

    # =====================
    # パブリックメソッド - メイン機能
    # =====================
    
    def prepare_csv_file_for_inference(self, local_csv_path: str) -> bool:
        """推論用CSVファイルを準備（Cloud Storageからダウンロード）"""
        try:
            self.logger.info("推論用CSVファイルの準備を開始します")
            
            # ファイル名を解決（GCS_FILE_NAMEが空の場合は最新のtimeseries_dataファイルを検索）
            if not self._resolve_target_file_name():
                return False
            
            if not self._validate_file_availability():
                return False
            
            self._log_file_information()
            
            success = self.download_file_to_local_path(local_csv_path)
            
            if success:
                self.logger.info("推論用CSVファイルの準備が完了しました")
            else:
                self.logger.error("推論用CSVファイルの準備に失敗しました")
            
            return success
                
        except Exception as e:
            self.logger.error(f"推論用CSVファイル準備中にエラーが発生: {str(e)}")
            return False

    def download_file_to_local_path(self, local_path: str) -> bool:
        """Cloud Storageからファイルをダウンロードしてローカルパスに保存"""
        try:
            self.logger.info(f"ファイルダウンロード開始: {self._get_gcs_uri()}")
            self.logger.info(f"ローカル保存先: {local_path}")
            
            blob = self._get_blob()
            if not self._check_blob_exists(blob):
                return False
            
            self._ensure_local_directory(local_path)
            blob.download_to_filename(local_path)
            
            file_size = os.path.getsize(local_path)
            self.logger.info(f"ファイルダウンロード完了: {file_size} bytes")
            return True
            
        except Exception as e:
            self.logger.error(f"ファイルダウンロード中にエラーが発生: {str(e)}")
            return False
    
    def download_file_as_bytes(self) -> Optional[bytes]:
        """Cloud Storageからファイルをバイト配列として取得"""
        try:
            self.logger.info(f"ファイルをバイト配列として取得開始: {self._get_gcs_uri()}")
            
            blob = self._get_blob()
            if not self._check_blob_exists(blob):
                return None
            
            file_content = blob.download_as_bytes()
            self.logger.info(f"ファイル取得完了: {len(file_content)} bytes")
            return file_content
            
        except Exception as e:
            self.logger.error(f"ファイル取得中にエラーが発生: {str(e)}")
            return None
    
    def download_file_to_temp_file(self) -> Optional[str]:
        """Cloud Storageからファイルを一時ファイルにダウンロード"""
        try:
            self.logger.info("ファイルを一時ファイルにダウンロード開始")
            
            file_content = self.download_file_as_bytes()
            if file_content is None:
                return None
            
            temp_path = self._create_temp_file(file_content)
            self.logger.info(f"一時ファイル作成完了: {temp_path}")
            return temp_path
            
        except Exception as e:
            self.logger.error(f"一時ファイル作成中にエラーが発生: {str(e)}")
            return None

    def file_exists(self) -> bool:
        """Cloud Storageにファイルが存在するかチェック"""
        try:
            blob = self._get_blob()
            exists = blob.exists()
            self.logger.info(f"ファイル存在チェック結果: {exists}")
            return exists
        except Exception as e:
            self.logger.error(f"ファイル存在チェック中にエラーが発生: {str(e)}")
            return False
    
    def get_file_metadata(self) -> Optional[Dict[str, Any]]:
        """ファイルのメタデータを取得"""
        try:
            blob = self._get_blob()
            if not self._check_blob_exists(blob):
                return None
            
            blob.reload()  # メタデータを最新状態に更新
            metadata = self._build_metadata_dict(blob)
            
            self.logger.info(f"ファイルメタデータ取得完了")
            self.logger.debug(f"メタデータ: {metadata}")
            return metadata
            
        except Exception as e:
            self.logger.error(f"メタデータ取得中にエラーが発生: {str(e)}")
            return None

    def cleanup_temp_file(self, temp_file_path: str) -> None:
        """一時ファイルを削除"""
        try:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
                self.logger.info(f"一時ファイルを削除しました: {temp_file_path}")
        except Exception as e:
            self.logger.warning(f"一時ファイルの削除に失敗: {str(e)}")

    # =====================
    # プライベートメソッド - ファイル検証・情報取得
    # =====================
    
    def _resolve_target_file_name(self) -> bool:
        """対象ファイル名を解決"""
        if self.config.gcs_file_name:
            # GCS_FILE_NAMEが指定されている場合はそのまま使用
            self._resolved_file_name = self.config.gcs_file_name
            self.logger.info(f"指定されたファイル名を使用: {self._resolved_file_name}")
            return True
        else:
            # GCS_FILE_NAMEが空の場合はtimeseries_dataプレフィックスで最新ファイルを検索
            self.logger.info("GCS_FILE_NAMEが空のため、timeseries_dataプレフィックスで最新ファイルを検索します")
            latest_file = self._find_latest_timeseries_file()
            if latest_file:
                self._resolved_file_name = latest_file
                self.logger.info(f"最新のtimeseries_dataファイルを使用: {self._resolved_file_name}")
                return True
            else:
                self.logger.error("timeseries_dataプレフィックスのファイルが見つかりませんでした")
                return False
    
    def _find_latest_timeseries_file(self) -> Optional[str]:
        """timeseries_dataプレフィックスの最新ファイルを検索"""
        try:
            bucket = self.client.bucket(self.config.gcs_bucket_name)
            
            # timeseries_dataプレフィックスでファイルをリスト
            blobs = bucket.list_blobs(prefix="timeseries_data")
            
            # 更新時間でソートして最新を取得
            latest_blob = None
            latest_time = None
            
            for blob in blobs:
                if blob.updated:
                    if latest_time is None or blob.updated > latest_time:
                        latest_time = blob.updated
                        latest_blob = blob
            
            if latest_blob:
                self.logger.info(f"最新ファイル発見: {latest_blob.name}, 更新時刻: {latest_time}")
                return latest_blob.name
            else:
                self.logger.warning("timeseries_dataプレフィックスのファイルが見つかりません")
                return None
                
        except Exception as e:
            self.logger.error(f"最新ファイル検索中にエラーが発生: {str(e)}")
            return None
    
    def _validate_file_availability(self) -> bool:
        """ファイルの利用可能性を検証"""
        if not self.file_exists():
            self.logger.error(f"Cloud Storageにファイルが存在しません: {self._get_gcs_uri()}")
            return False
        return True
    
    def _log_file_information(self) -> None:
        """ファイル情報をログ出力"""
        metadata = self.get_file_metadata()
        if metadata:
            self.logger.info(
                f"ファイル情報 - サイズ: {metadata.get('size', 'N/A')} bytes, "
                f"更新日時: {metadata.get('updated', 'N/A')}"
            )
    
    def _check_blob_exists(self, blob: storage.Blob) -> bool:
        """Blobの存在確認とエラーハンドリング"""
        try:
            if not blob.exists():
                self.logger.error(f"ファイルが存在しません: {self._get_gcs_uri()}")
                return False
            return True
        except NotFound:
            self.logger.error(f"バケットまたはファイルが見つかりません: {self._get_gcs_uri()}")
            return False
        except Forbidden:
            self.logger.error(f"アクセス権限がありません: {self._get_gcs_uri()}")
            return False
    
    def _build_metadata_dict(self, blob: storage.Blob) -> Dict[str, Any]:
        """メタデータ辞書を構築"""
        return {
            "name": blob.name,
            "bucket": blob.bucket.name,
            "size": blob.size,
            "content_type": blob.content_type,
            "created": blob.time_created.isoformat() if blob.time_created else None,
            "updated": blob.updated.isoformat() if blob.updated else None,
            "etag": blob.etag,
            "md5_hash": blob.md5_hash,
        }

    # =====================
    # プライベートメソッド - Cloud Storage操作
    # =====================
    
    def _get_blob(self) -> storage.Blob:
        """Cloud StorageのBlobオブジェクトを取得"""
        bucket = self.client.bucket(self.config.gcs_bucket_name)
        file_name = self._resolved_file_name or self.config.gcs_file_name
        return bucket.blob(file_name)
    
    def _get_gcs_uri(self) -> str:
        """Cloud Storage URIを取得"""
        file_name = self._resolved_file_name or self.config.gcs_file_name
        return f"gs://{self.config.gcs_bucket_name}/{file_name}"

    # =====================
    # プライベートメソッド - ローカルファイル操作
    # =====================
    
    def _ensure_local_directory(self, local_path: str) -> None:
        """ローカルファイルの保存先ディレクトリを作成"""
        directory = os.path.dirname(local_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            self.logger.info(f"ディレクトリを作成しました: {directory}")
    
    def _create_temp_file(self, content: bytes) -> str:
        """一時ファイルを作成"""
        temp_file = tempfile.NamedTemporaryFile(
            delete=False, 
            suffix=self._get_file_extension()
        )
        temp_file.write(content)
        temp_file.close()
        return temp_file.name
    
    def _get_file_extension(self) -> str:
        """ファイル拡張子を取得"""
        file_name = self._resolved_file_name or self.config.gcs_file_name
        _, extension = os.path.splitext(file_name)
        return extension if extension else '.tmp'