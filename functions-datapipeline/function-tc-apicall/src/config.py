import os
from dataclasses import dataclass


@dataclass
class Config:
    """アプリケーション設定"""

    tenant_domain: str
    authorization: str
    source: str

    @classmethod
    def from_environment(cls) -> "Config":
        """環境変数から設定を読み込み"""
        return cls(
            tenant_domain=os.environ.get("TENANT_DOMAIN", ""),
            authorization=os.environ.get("AUTHORIZATION", ""),
            source=os.environ.get("SOURCE", ""),
        )

    def validate(self) -> None:
        """設定の妥当性チェック"""
        if not self.tenant_domain:
            raise ValueError("TENANT_DOMAIN環境変数が設定されていません")
        if not self.authorization:
            raise ValueError("AUTHORIZATION環境変数が設定されていません")
        if not self.source:
            raise ValueError("SOURCE環境変数が設定されていません")