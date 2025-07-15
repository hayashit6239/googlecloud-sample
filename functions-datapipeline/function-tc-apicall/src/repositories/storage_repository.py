import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

from google.cloud import storage


class StorageRepository(ABC):
    """ストレージ操作の抽象インターフェース"""

    @abstractmethod
    def upload_file(self, local_file_path: str, destination_path: str) -> str:
        """ファイルをアップロードする"""
        pass


class CloudStorageRepository(StorageRepository):
    """Google Cloud Storage へのファイルアップロード"""

    def __init__(self, bucket_name: str, project_id: Optional[str] = None):
        """
        CloudStorageRepositoryを初期化
        
        Args:
            bucket_name: Cloud Storage バケット名
            project_id: Google Cloud プロジェクトID（省略時は環境から取得）
        """
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Cloud Storage クライアントを初期化
        if project_id:
            self.storage_client = storage.Client(project=project_id)
        else:
            self.storage_client = storage.Client()

    def upload_file(self, local_file_path: str, destination_path: str) -> str:
        """
        ローカルファイルをCloud Storageにアップロード
        
        Args:
            local_file_path: アップロードするローカルファイルパス
            destination_path: Cloud Storage内のファイルパス
            
        Returns:
            アップロードされたファイルのURL
        """
        self.logger.info(f"Cloud Storageアップロード開始: {destination_path}")
        
        try:
            # バケットとblobオブジェクトを取得
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(destination_path)
            
            # ファイルをアップロード
            blob.upload_from_filename(local_file_path)
            
            # アップロードされたファイルのURL
            file_url = f"gs://{self.bucket_name}/{destination_path}"
            self.logger.info(f"Cloud Storageアップロード完了: {file_url}")
            
            return file_url
            
        except Exception as e:
            self.logger.error(f"Cloud Storageアップロード失敗: {e}")
            raise e


class LocalStorageRepository(StorageRepository):
    """ローカルファイルシステムでのファイル操作（テスト用）"""

    def __init__(self, base_path: str):
        self.base_path = base_path
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def upload_file(self, local_file_path: str, destination_path: str) -> str:
        """
        ローカルファイルを指定パスにコピー
        
        Args:
            local_file_path: コピー元ファイルパス
            destination_path: コピー先ファイルパス
            
        Returns:
            コピーされたファイルのパス
        """
        import shutil
        import os
        
        full_destination_path = os.path.join(self.base_path, destination_path)
        
        # ディレクトリが存在しない場合は作成
        os.makedirs(os.path.dirname(full_destination_path), exist_ok=True)
        
        shutil.copy2(local_file_path, full_destination_path)
        self.logger.info(f"ローカルファイルコピー完了: {full_destination_path}")
        
        return full_destination_path