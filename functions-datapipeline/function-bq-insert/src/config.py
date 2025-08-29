import os
from dataclasses import dataclass


@dataclass
class Config:
    """BigQuery挿入機能の設定"""

    project_id: str
    dataset_id: str
    table_id: str
    
    # NodeAI API設定
    nodeai_api_key: str
    nodeai_api_id: str
    nodeai_base_url: str
    csv_file_path: str
    threshold: float
    tc_data_delay_minutes: int

    @classmethod
    def from_environment(cls) -> "Config":
        """環境変数から設定を読み込み"""
        return cls(
            project_id=os.environ.get("PROJECT_ID", ""),
            dataset_id=os.environ.get("DATASET_ID", ""),
            table_id=os.environ.get("TABLE_ID", ""),
            nodeai_api_key=os.environ.get("NODEAI_API_KEY", ""),
            nodeai_api_id=os.environ.get("NODEAI_API_ID", ""),
            nodeai_base_url=os.environ.get("NODEAI_BASE_URL", ""),
            csv_file_path=os.environ.get("CSV_FILE_PATH", ""),
            threshold=float(os.environ.get("THRESHOLD", "1.0")),
            tc_data_delay_minutes=int(os.environ.get("TC_DATA_DELAY_MINUTES", "5")),
        )

    def validate(self) -> None:
        """設定の妥当性チェック"""
        if not self.project_id:
            raise ValueError("PROJECT_ID環境変数が設定されていません")
        if not self.dataset_id:
            raise ValueError("DATASET_ID環境変数が設定されていません")
        if not self.table_id:
            raise ValueError("TABLE_ID環境変数が設定されていません")
        if not self.nodeai_api_key:
            raise ValueError("NODEAI_API_KEY環境変数が設定されていません")
        if not self.nodeai_api_id:
            raise ValueError("NODEAI_API_ID環境変数が設定されていません")
        if not self.nodeai_base_url:
            raise ValueError("NODEAI_BASE_URL環境変数が設定されていません")
        if not self.csv_file_path:
            raise ValueError("CSV_FILE_PATH環境変数が設定されていません")
    
    def get_env_var(self, key: str) -> str:
        """環境変数を取得"""
        return os.environ.get(key, "")