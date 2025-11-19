"""
Sector Rotation Chart Generator

セクターローテーションチャートを生成し、週次・月次のIndustry Group RSを
4象限チャート（Strong, Improving, Weakening, Weak）として可視化します。
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
import numpy as np
from ibd_database import IBDDatabase
import io
from PIL import Image


class SectorRotationChart:
    """セクターローテーションチャート生成クラス"""

    def __init__(self, db_path='ibd_data.db'):
        self.db = IBDDatabase(db_path, silent=True)

    def generate_chart(self, output_path='sector_rotation.png', dpi=150):
        """
        セクターローテーションチャートを生成

        Args:
            output_path: 出力ファイルパス
            dpi: 画像解像度

        Returns:
            str: 出力ファイルパス
        """
        # データ取得
        df = self.db.get_sector_rotation_data()

        if df is None or len(df) == 0:
            print("エラー: セクターローテーションデータが見つかりません")
            return None

        # 週次・月次RS値を0-100のスコアに正規化（パーセンタイルランキング）
        df['Weekly_RS'] = self._normalize_to_percentile(df['weekly_rs'])
        df['Monthly_RS'] = self._normalize_to_percentile(df['monthly_rs'])

        # Themeカラムを作成（業界名を使用）
        df['Theme'] = df['industry']

        # プロット作成
        self._create_plot(df, output_path, dpi)

        print(f"セクターローテーションチャートを生成: {output_path}")
        return output_path

    def _normalize_to_percentile(self, values: pd.Series) -> pd.Series:
        """値をパーセンタイルランキング（0-100）に変換"""
        return values.rank(pct=True) * 100

    def _create_plot(self, df: pd.DataFrame, output_path: str, dpi: int):
        """プロット作成"""
        fig, ax = plt.subplots(figsize=(14, 8))

        # 軸の範囲設定
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)

        # タイトルとラベル
        ax.set_title('Industry Group RS Change\nWeekly Vs Monthly',
                     fontsize=16, fontweight='bold')
        ax.set_xlabel('Weekly RS Change (Short Term)', fontsize=12)
        ax.set_ylabel('Monthly RS Change (Medium Term)', fontsize=12)

        # 背景色（象限）の描画
        # 左上: Weakening (Pink)
        rect_ul = patches.Rectangle((0, 50), 50, 50, linewidth=0,
                                    edgecolor='none', facecolor='#ffebee', alpha=1)
        # 左下: Weak (Darker Pink)
        rect_ll = patches.Rectangle((0, 0), 50, 50, linewidth=0,
                                    edgecolor='none', facecolor='#ffcdd2', alpha=1)
        # 右上: Strong (Green)
        rect_ur = patches.Rectangle((50, 50), 50, 50, linewidth=0,
                                    edgecolor='none', facecolor='#c8e6c9', alpha=1)
        # 右下: Improving (Lighter Green)
        rect_lr = patches.Rectangle((50, 0), 50, 50, linewidth=0,
                                    edgecolor='none', facecolor='#e8f5e9', alpha=1)

        ax.add_patch(rect_ul)
        ax.add_patch(rect_ll)
        ax.add_patch(rect_ur)
        ax.add_patch(rect_lr)

        # 象限のラベル表示
        ax.text(25, 75, 'Weakening', fontsize=20, fontweight='bold',
               ha='center', va='center', alpha=0.7)
        ax.text(25, 25, 'Weak', fontsize=20, fontweight='bold',
               ha='center', va='center', alpha=0.7)
        ax.text(75, 75, 'Strong', fontsize=20, fontweight='bold',
               ha='center', va='center', alpha=0.7)
        ax.text(75, 25, 'Improving', fontsize=20, fontweight='bold',
               ha='center', va='center', alpha=0.7)

        # グリッド線
        ax.grid(True, linestyle=':', alpha=0.6, color='gray')
        ax.axhline(50, color='gray', linewidth=1)
        ax.axvline(50, color='gray', linewidth=1)

        # データのプロット
        scatter = ax.scatter(df['Weekly_RS'], df['Monthly_RS'],
                           color='#3f51b5', s=50, zorder=10)

        # テキストラベルの追加
        texts = []
        for i, txt in enumerate(df['Theme']):
            texts.append(ax.text(df['Weekly_RS'].iloc[i] + 1,
                               df['Monthly_RS'].iloc[i],
                               txt, fontsize=9, zorder=11))

        # 重なり防止
        try:
            from adjustText import adjust_text
            adjust_text(texts, arrowprops=dict(arrowstyle='-',
                                              color='gray', lw=0.5))
        except ImportError:
            print("adjustTextライブラリがないため、単純なラベル配置を行います。")

        plt.tight_layout()
        plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
        plt.close()

    def generate_chart_as_bytes(self, dpi=150) -> bytes:
        """
        チャートを画像バイトデータとして生成（Googleスプレッドシート用）

        Returns:
            bytes: PNG画像データ
        """
        # 一時的にメモリ上に保存
        buf = io.BytesIO()

        df = self.db.get_sector_rotation_data()
        if df is None or len(df) == 0:
            return None

        df['Weekly_RS'] = self._normalize_to_percentile(df['weekly_rs'])
        df['Monthly_RS'] = self._normalize_to_percentile(df['monthly_rs'])
        df['Theme'] = df['industry']

        self._create_plot_to_buffer(df, buf, dpi)

        buf.seek(0)
        return buf.getvalue()

    def _create_plot_to_buffer(self, df: pd.DataFrame, buf: io.BytesIO, dpi: int):
        """プロットをバッファに作成"""
        fig, ax = plt.subplots(figsize=(14, 8))

        # 軸の範囲設定
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)

        # タイトルとラベル
        ax.set_title('Industry Group RS Change\nWeekly Vs Monthly',
                     fontsize=16, fontweight='bold')
        ax.set_xlabel('Weekly RS Change (Short Term)', fontsize=12)
        ax.set_ylabel('Monthly RS Change (Medium Term)', fontsize=12)

        # 背景色（象限）の描画
        rect_ul = patches.Rectangle((0, 50), 50, 50, linewidth=0,
                                    edgecolor='none', facecolor='#ffebee', alpha=1)
        rect_ll = patches.Rectangle((0, 0), 50, 50, linewidth=0,
                                    edgecolor='none', facecolor='#ffcdd2', alpha=1)
        rect_ur = patches.Rectangle((50, 50), 50, 50, linewidth=0,
                                    edgecolor='none', facecolor='#c8e6c9', alpha=1)
        rect_lr = patches.Rectangle((50, 0), 50, 50, linewidth=0,
                                    edgecolor='none', facecolor='#e8f5e9', alpha=1)

        ax.add_patch(rect_ul)
        ax.add_patch(rect_ll)
        ax.add_patch(rect_ur)
        ax.add_patch(rect_lr)

        # 象限のラベル表示
        ax.text(25, 75, 'Weakening', fontsize=20, fontweight='bold',
               ha='center', va='center', alpha=0.7)
        ax.text(25, 25, 'Weak', fontsize=20, fontweight='bold',
               ha='center', va='center', alpha=0.7)
        ax.text(75, 75, 'Strong', fontsize=20, fontweight='bold',
               ha='center', va='center', alpha=0.7)
        ax.text(75, 25, 'Improving', fontsize=20, fontweight='bold',
               ha='center', va='center', alpha=0.7)

        # グリッド線
        ax.grid(True, linestyle=':', alpha=0.6, color='gray')
        ax.axhline(50, color='gray', linewidth=1)
        ax.axvline(50, color='gray', linewidth=1)

        # データのプロット
        scatter = ax.scatter(df['Weekly_RS'], df['Monthly_RS'],
                           color='#3f51b5', s=50, zorder=10)

        # テキストラベルの追加
        texts = []
        for i, txt in enumerate(df['Theme']):
            texts.append(ax.text(df['Weekly_RS'].iloc[i] + 1,
                               df['Monthly_RS'].iloc[i],
                               txt, fontsize=9, zorder=11))

        # 重なり防止
        try:
            from adjustText import adjust_text
            adjust_text(texts, arrowprops=dict(arrowstyle='-',
                                              color='gray', lw=0.5))
        except ImportError:
            pass

        plt.tight_layout()
        plt.savefig(buf, format='png', dpi=dpi, bbox_inches='tight')
        plt.close()

    def close(self):
        """リソースをクリーンアップ"""
        self.db.close()


def main():
    """テスト実行"""
    try:
        chart_generator = SectorRotationChart()

        # チャート生成
        output_path = chart_generator.generate_chart(output_path='sector_rotation.png', dpi=150)

        if output_path:
            print(f"\nチャートが生成されました: {output_path}")
        else:
            print("\nチャート生成に失敗しました（データがありません）")

        chart_generator.close()

    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
