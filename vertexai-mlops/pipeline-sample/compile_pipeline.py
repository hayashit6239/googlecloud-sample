#!/usr/bin/env python3
"""
パイプラインをコンパイルするスクリプト

パイプライン定義を YAML ファイルにコンパイルします。

Requires: Python 3.11+
"""

import argparse
import sys
from pathlib import Path

from kfp import compiler

# 親ディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent))

from pipelines.training_pipeline import ml_training_pipeline, simple_pipeline


def compile_pipeline(
    pipeline_name: str,
    output_path: str | None = None,
) -> str:
    """
    パイプラインをコンパイルする

    Args:
        pipeline_name: パイプライン名（ml_training, simple）
        output_path: 出力ファイルパス（省略時は自動生成）

    Returns:
        コンパイルされた YAML ファイルのパス
    """
    # パイプラインを選択
    pipelines = {
        "ml_training": ml_training_pipeline,
        "simple": simple_pipeline,
    }

    if pipeline_name not in pipelines:
        raise ValueError(
            f"不明なパイプライン: {pipeline_name}\n"
            f"利用可能なパイプライン: {list(pipelines.keys())}"
        )

    pipeline_func = pipelines[pipeline_name]

    # 出力パスを決定
    if output_path is None:
        output_dir = Path(__file__).parent / "compiled"
        output_dir.mkdir(exist_ok=True)
        output_path = str(output_dir / f"{pipeline_name}_pipeline.yaml")

    # コンパイル
    print(f"パイプラインをコンパイル中: {pipeline_name}")
    compiler.Compiler().compile(
        pipeline_func=pipeline_func,
        package_path=output_path,
    )

    print(f"コンパイル完了: {output_path}")
    return output_path


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description="パイプラインをコンパイルする"
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default="simple",
        choices=["ml_training", "simple"],
        help="コンパイルするパイプライン（デフォルト: simple）",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="出力ファイルパス（省略時は compiled/ に保存）",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="すべてのパイプラインをコンパイル",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("パイプラインコンパイラ")
    print("=" * 60)

    if args.all:
        # すべてのパイプラインをコンパイル
        for pipeline_name in ["ml_training", "simple"]:
            compile_pipeline(pipeline_name)
        print("\nすべてのパイプラインをコンパイルしました")
    else:
        # 指定されたパイプラインをコンパイル
        compile_pipeline(args.pipeline, args.output)

    print("=" * 60)


if __name__ == "__main__":
    main()
