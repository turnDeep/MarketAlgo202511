"""
FinancialModelingPrep API Stock Screener を使用して純粋な個別銘柄を取得

yfinanceの代わりにFMP Stock Screener APIを使用することで：
- ETF/投資信託を自動的に除外（isEtf=false, isFund=false）
- 複雑な文字列フィルタリングが不要
- 処理時間を数分〜十数分から数秒に短縮
- より正確な銘柄分類

環境変数 FMP_API_KEY が必要です。
"""

import os
import pandas as pd
from typing import List, Dict
import time
from curl_cffi.requests import Session
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()


class FMPTickerFetcher:
    """FMP Stock Screener API を使用してティッカーを取得"""

    BASE_URL = "https://financialmodelingprep.com/api/v3/stock-screener"

    def __init__(self, api_key: str = None, rate_limit: int = None):
        """
        FMP Ticker Fetcherの初期化

        Args:
            api_key: FMP API Key（環境変数 FMP_API_KEY から自動取得可能）
            rate_limit: 1分あたりのAPIレート制限（環境変数 FMP_RATE_LIMIT から自動取得可能）
        """
        self.api_key = api_key or os.getenv('FMP_API_KEY')
        if not self.api_key:
            raise ValueError(
                "FMP API Key is required. Set FMP_API_KEY environment variable "
                "or pass api_key parameter."
            )

        # レート制限の設定（環境変数から取得、デフォルトは750 req/min - Premium Plan）
        self.rate_limit = rate_limit or int(os.getenv('FMP_RATE_LIMIT', '750'))
        self.session = Session(impersonate="chrome110")
        self.request_timestamps = []

    def _enforce_rate_limit(self):
        """設定されたAPIレート制限を適用"""
        current_time = time.time()
        # 60秒以上前のタイムスタンプを削除
        self.request_timestamps = [t for t in self.request_timestamps if current_time - t < 60]

        if len(self.request_timestamps) >= self.rate_limit:
            # 最も古いリクエストから60秒経過するまで待機
            sleep_time = 60 - (current_time - self.request_timestamps[0]) + 0.1
            print(f"レート制限に達しました。{sleep_time:.1f}秒待機します...")
            time.sleep(sleep_time)
            # 待機後、再度古いタイムスタンプを削除
            current_time = time.time()
            self.request_timestamps = [t for t in self.request_timestamps if current_time - t < 60]

        self.request_timestamps.append(current_time)

    def _make_request(self, params: Dict) -> List[Dict]:
        """
        エラーハンドリングとレート制限を考慮したAPIリクエストを実行

        Args:
            params: クエリパラメータ

        Returns:
            List[Dict]: JSONレスポンス（辞書のリスト）
        """
        self._enforce_rate_limit()

        params['apikey'] = self.api_key

        try:
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            else:
                raise ValueError(f"Unexpected response format: {data}")

        except Exception as e:
            print(f"API request failed: {e}")
            return []

    def get_stocks_by_exchange(self, exchange: str) -> List[Dict]:
        """
        指定された取引所から純粋な個別銘柄を取得

        Args:
            exchange: 取引所名 ('nasdaq', 'nyse', 'amex' など)

        Returns:
            銘柄情報のリスト
        """
        params = {
            'isEtf': 'false',              # ETF除外
            'isFund': 'false',             # 投資信託除外
            'isActivelyTrading': 'true',   # 取引停止中を除外
            'exchange': exchange.lower(),
            'limit': 10000                 # 最大取得数
        }

        print(f"\n{exchange.upper()} の銘柄を取得中...")
        stocks = self._make_request(params)
        print(f"  {len(stocks)} 件の銘柄を取得しました")

        return stocks

    def get_all_stocks(self, exchanges: List[str] = None) -> pd.DataFrame:
        """
        指定された取引所から全ての個別銘柄を取得

        Args:
            exchanges: 取引所のリスト（デフォルト: ['nasdaq', 'nyse', 'amex']）

        Returns:
            銘柄情報を含むDataFrame
        """
        if exchanges is None:
            exchanges = ['nasdaq', 'nyse', 'amex']

        all_stocks = []

        for exchange in exchanges:
            stocks = self.get_stocks_by_exchange(exchange)

            for stock in stocks:
                all_stocks.append({
                    'Ticker': stock.get('symbol'),
                    'Exchange': exchange.upper(),
                    'CompanyName': stock.get('companyName', ''),
                    'MarketCap': stock.get('marketCap', 0),
                    'Sector': stock.get('sector', ''),
                    'Industry': stock.get('industry', ''),
                    'Country': stock.get('country', '')
                })

        df = pd.DataFrame(all_stocks)

        # 重複除外（同じティッカーが複数の取引所にリストされている場合）
        # 最初に見つかった取引所を優先
        df.drop_duplicates(subset=['Ticker'], keep='first', inplace=True)

        return df


def get_and_save_tickers():
    """
    FMP Stock Screener APIからNASDAQ、NYSE、AMEXの純粋な個別銘柄を取得し、
    stock.csvに保存します。

    従来のyfinanceベースの実装と比較して：
    - 処理時間: 数分〜十数分 → 数秒に短縮
    - 精度: FMP側で分類されているため、より正確
    - シンプル: 複雑な文字列フィルタリングが不要
    """
    print("=" * 60)
    print("FMP Stock Screener API を使用してティッカーリストを取得")
    print("=" * 60)

    try:
        # FMPTickerFetcherの初期化
        fetcher = FMPTickerFetcher()

        # 取引所リスト
        exchanges = ['nasdaq', 'nyse']

        print(f"\n取得対象取引所: {', '.join([e.upper() for e in exchanges])}")
        print(f"フィルター条件:")
        print(f"  - isEtf: false (ETF除外)")
        print(f"  - isFund: false (投資信託除外)")
        print(f"  - isActivelyTrading: true (取引停止中を除外)")

        # 全銘柄を取得
        df = fetcher.get_all_stocks(exchanges)

        print(f"\n" + "=" * 60)
        print(f"取得結果:")
        print(f"  合計銘柄数: {len(df)} 件")

        # 取引所別の内訳
        print(f"\n取引所別内訳:")
        for exchange in exchanges:
            count = len(df[df['Exchange'] == exchange.upper()])
            print(f"  {exchange.upper()}: {count} 件")

        # セクター別の内訳（上位10件）
        if 'Sector' in df.columns and not df['Sector'].isna().all():
            print(f"\nセクター別内訳（上位10件）:")
            sector_counts = df['Sector'].value_counts().head(10)
            for sector, count in sector_counts.items():
                if sector:  # 空文字列を除外
                    print(f"  {sector}: {count} 件")

        # stock.csvに保存（Ticker, Exchangeのみ）
        output_df = df[['Ticker', 'Exchange']].copy()
        output_df.to_csv('stock.csv', index=False)
        print(f"\n✓ ティッカーリストを stock.csv に保存しました")

        # オプション: 詳細情報を含むCSVも保存
        df.to_csv('stock_detailed.csv', index=False)
        print(f"✓ 詳細情報を stock_detailed.csv に保存しました")

        print("=" * 60)
        print("処理が正常に完了しました！")
        print("=" * 60)

    except ValueError as e:
        print(f"\nエラー: {e}")
        print("環境変数 FMP_API_KEY が正しく設定されているか確認してください。")
        return
    except Exception as e:
        print(f"\nエラー: 予期しないエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return


if __name__ == '__main__':
    get_and_save_tickers()
