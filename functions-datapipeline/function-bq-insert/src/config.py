import os
from dataclasses import dataclass


@dataclass
class Config:
    """BigQuery挿入機能の設定"""

    project_id: str
    dataset_id: str
    table_id: str

    @classmethod
    def from_environment(cls) -> "Config":
        """環境変数から設定を読み込み"""
        return cls(
            project_id=os.environ.get("PROJECT_ID", ""),
            dataset_id=os.environ.get("DATASET_ID", ""),
            table_id=os.environ.get("TABLE_ID", ""),
        )

    def validate(self) -> None:
        """設定の妥当性チェック"""
        if not self.project_id:
            raise ValueError("PROJECT_ID環境変数が設定されていません")
        if not self.dataset_id:
            raise ValueError("DATASET_ID環境変数が設定されていません")
        if not self.table_id:
            raise ValueError("TABLE_ID環境変数が設定されていません")
    
    def get_env_var(self, key: str) -> str:
        """環境変数を取得"""
        return os.environ.get(key, "")