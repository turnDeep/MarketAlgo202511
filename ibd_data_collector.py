"""
IBD Data Collector

全銘柄の株価データ、EPSデータ、企業プロファイルをFMP APIから取得し、
SQLiteデータベースに保存します。
"""

from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from curl_cffi.requests import Session
import pandas as pd
import numpy as np

from ibd_database import IBDDatabase
from ibd_utils import RateLimiter
from get_tickers import FMPTickerFetcher


class IBDDataCollector:
    """IBDスクリーナー用のデータ収集クラス"""

    def __init__(self, fmp_api_key: str, db_path: str = 'ibd_data.db', debug: bool = False):
        """
        Args:
            fmp_api_key: Financial Modeling Prep API Key
            db_path: データベースファイルのパス
            debug: デバッグモードを有効にする
        """
        self.fmp_api_key = fmp_api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.rate_limiter = RateLimiter(max_calls_per_minute=750)
        self.db_path = db_path
        self.db = IBDDatabase(self.db_path, silent=False)
        self.debug = debug

    def fetch_with_rate_limit(self, url: str, params: dict = None) -> Optional[dict]:
        """レート制限を考慮したAPIリクエスト"""
        self.rate_limiter.wait_if_needed()

        if params is None:
            params = {}
        params['apikey'] = self.fmp_api_key

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if self.debug:  # デバッグモードの時だけエラー表示
                print(f"    API Error: {url} - {str(e)}")
            return None

    # ==================== データ取得メソッド ====================

    def get_historical_prices(self, symbol: str, days: int = 300) -> Optional[pd.DataFrame]:
        """過去の価格データを取得"""
        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {'timeseries': days}

        data = self.fetch_with_rate_limit(url, params)

        if data and 'historical' in data and data['historical']:
            df = pd.DataFrame(data['historical'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            return df
        return None

    def get_income_statement(self, symbol: str, period: str = 'quarter', limit: int = 8) -> Optional[List[Dict]]:
        """損益計算書を取得"""
        url = f"{self.base_url}/income-statement/{symbol}"
        params = {'period': period, 'limit': limit}
        data = self.fetch_with_rate_limit(url, params)
        return data if data else None

    def get_balance_sheet(self, symbol: str, period: str = 'annual', limit: int = 5) -> Optional[List[Dict]]:
        """貸借対照表を取得"""
        url = f"{self.base_url}/balance-sheet-statement/{symbol}"
        params = {'period': period, 'limit': limit}
        data = self.fetch_with_rate_limit(url, params)
        return data if data else None

    def get_company_profile(self, symbol: str) -> Optional[Dict]:
        """企業プロファイルを取得"""
        url = f"{self.base_url}/profile/{symbol}"
        data = self.fetch_with_rate_limit(url)

        if data and len(data) > 0:
            return data[0]
        return None

    def get_historical_sector_performance(self, limit: int = 300) -> Optional[List[Dict]]:
        """履歴セクターパフォーマンスを取得"""
        url = f"{self.base_url}/historical-sectors-performance"
        params = {'limit': limit}
        data = self.fetch_with_rate_limit(url, params)
        return data if data else None

    def get_current_sector_performance(self) -> Optional[List[Dict]]:
        """現在のセクターパフォーマンスを取得"""
        url = f"{self.base_url}/sectors-performance"
        data = self.fetch_with_rate_limit(url)
        return data if data else None

    # ==================== データ収集（単一銘柄） ====================

    def collect_ticker_data(self, ticker: str, db_conn: IBDDatabase) -> bool:
        """
        単一銘柄の全データを収集してDBに保存（スレッドセーフ）
        """
        try:
            # 1. 株価データ取得
            prices_df = self.get_historical_prices(ticker, days=300)
            if prices_df is not None and len(prices_df) >= 252:
                db_conn.insert_price_history(ticker, prices_df)
            else:
                if self.debug:
                    print(f"    {ticker}: 株価データ不足 (取得: {len(prices_df) if prices_df is not None else 0}日)")
                return False

            # 2. 四半期損益計算書取得
            income_q = self.get_income_statement(ticker, period='quarter', limit=8)
            if income_q and len(income_q) >= 5:
                db_conn.insert_income_statements_quarterly(ticker, income_q)
            else:
                if self.debug:
                    print(f"    {ticker}: 四半期データ不足 (取得: {len(income_q) if income_q else 0}期)")
                return False

            # 3. 年次損益計算書取得
            income_a = self.get_income_statement(ticker, period='annual', limit=5)
            if income_a:
                db_conn.insert_income_statements_annual(ticker, income_a)

            # 4. 年次貸借対照表取得（ROE計算に使用）
            balance_sheet = self.get_balance_sheet(ticker, period='annual', limit=5)
            if balance_sheet:
                db_conn.insert_balance_sheet_annual(ticker, balance_sheet)

            # 5. 企業プロファイル取得
            profile = self.get_company_profile(ticker)
            if profile:
                db_conn.insert_company_profile(ticker, profile)

            return True

        except Exception as e:
            if self.debug:
                print(f"    {ticker}: エラー - {str(e)}")
            return False

    # ==================== 並列データ収集 ====================

    def collect_batch(self, tickers_batch: List[str]) -> Dict:
        """
        ティッカーのバッチを処理（スレッドごとにDB接続）
        """
        db_conn = IBDDatabase(self.db_path, silent=True)
        results = {
            'success': 0,
            'failed': 0,
            'tickers_collected': []
        }
        try:
            for ticker in tickers_batch:
                if self.collect_ticker_data(ticker, db_conn):
                    results['success'] += 1
                    results['tickers_collected'].append(ticker)
                else:
                    results['failed'] += 1
        finally:
            db_conn.close()

        return results

    def collect_all_data(self, tickers_list: List[str], max_workers: int = 3):
        """
        全銘柄のデータを並列収集

        Args:
            tickers_list: ティッカーリスト
            max_workers: 最大ワーカー数（デフォルト3: 750 calls/min制限に対応）
        """
        print(f"\n{'='*80}")
        print(f"全銘柄のデータ収集開始（{len(tickers_list)} 銘柄）")
        print(f"並列ワーカー数: {max_workers} (レート制限: 750 calls/min)")
        print(f"{'='*80}")

        # バッチサイズを設定
        batch_size = 50
        batches = [tickers_list[i:i+batch_size] for i in range(0, len(tickers_list), batch_size)]

        total_success = 0
        total_failed = 0
        all_collected_tickers = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # バッチごとに処理を投入
            future_to_batch = {executor.submit(self.collect_batch, batch): batch for batch in batches}

            completed = 0
            for future in as_completed(future_to_batch):
                completed += 1
                try:
                    batch_results = future.result()
                    total_success += batch_results['success']
                    total_failed += batch_results['failed']
                    all_collected_tickers.extend(batch_results['tickers_collected'])

                    if completed % 10 == 0 or completed == len(batches):
                        print(f"  進捗: {completed}/{len(batches)} バッチ完了")
                        print(f"    成功: {total_success} 銘柄, 失敗: {total_failed} 銘柄")
                except Exception as e:
                    continue

        print(f"\n{'='*80}")
        print(f"データ収集完了")
        print(f"  成功: {total_success} 銘柄")
        print(f"  失敗: {total_failed} 銘柄")
        print(f"{'='*80}\n")

        # 成功したティッカーをティッカーマスターに追加
        tickers_data = [{'ticker': t, 'exchange': None, 'name': None} for t in all_collected_tickers]
        self.db.insert_tickers_bulk(tickers_data)

        return all_collected_tickers

    # ==================== RS値の計算と保存 ====================

    def calculate_and_store_rs_values(self, tickers_list: List[str] = None):
        """
        全銘柄のRS値を計算してDBに保存

        Args:
            tickers_list: ティッカーリスト（Noneの場合はDB内の全銘柄）
        """
        if tickers_list is None:
            tickers_list = self.db.get_all_tickers()

        print(f"\n全銘柄のRS値を計算中（{len(tickers_list)} 銘柄）...")

        success_count = 0
        for idx, ticker in enumerate(tickers_list):
            if (idx + 1) % 500 == 0:
                print(f"  進捗: {idx + 1}/{len(tickers_list)} 銘柄")

            try:
                prices_df = self.db.get_price_history(ticker, days=300)
                if prices_df is None or len(prices_df) < 252:
                    continue

                # RS値を計算
                rs_value, roc_63d, roc_126d, roc_189d, roc_252d = self.calculate_rs_value(prices_df)
                if rs_value is not None:
                    self.db.insert_calculated_rs(ticker, rs_value, roc_63d, roc_126d, roc_189d, roc_252d)
                    success_count += 1
            except Exception as e:
                continue

        print(f"  {success_count} 銘柄のRS値を計算しました\n")

    def calculate_rs_value(self, prices_df: pd.DataFrame) -> tuple:
        """
        RS値を計算（IBD方式）

        Returns:
            tuple: (rs_value, roc_63d, roc_126d, roc_189d, roc_252d)
        """
        if prices_df is None or len(prices_df) < 252:
            return None, None, None, None, None

        try:
            close = prices_df['close'].values

            # 各期間のROC（Rate of Change）を計算
            roc_63d = (close[-1] / close[-63] - 1) * 100 if close[-63] != 0 else 0
            roc_126d = (close[-1] / close[-126] - 1) * 100 if close[-126] != 0 else 0
            roc_189d = (close[-1] / close[-189] - 1) * 100 if close[-189] != 0 else 0
            roc_252d = (close[-1] / close[-252] - 1) * 100 if close[-252] != 0 else 0

            # IBD式の加重平均（最新四半期に40%の重み）
            rs_value = 0.4 * roc_63d + 0.2 * roc_126d + 0.2 * roc_189d + 0.2 * roc_252d

            return rs_value, roc_63d, roc_126d, roc_189d, roc_252d

        except Exception as e:
            return None, None, None, None, None

    # ==================== EPS要素の計算と保存 ====================

    def calculate_and_store_eps_components(self, tickers_list: List[str] = None):
        """
        全銘柄のEPS要素を計算してDBに保存

        Args:
            tickers_list: ティッカーリスト（Noneの場合はDB内の全銘柄）
        """
        if tickers_list is None:
            tickers_list = self.db.get_all_tickers()

        print(f"\n全銘柄のEPS要素を計算中（{len(tickers_list)} 銘柄）...")

        success_count = 0
        for idx, ticker in enumerate(tickers_list):
            if (idx + 1) % 500 == 0:
                print(f"  進捗: {idx + 1}/{len(tickers_list)} 銘柄")

            try:
                income_q = self.db.get_income_statements_quarterly(ticker, limit=8)
                income_a = self.db.get_income_statements_annual(ticker, limit=5)

                if not income_q or len(income_q) < 5:
                    continue

                # EPS要素を計算
                eps_components = self.calculate_eps_components(income_q, income_a)
                if eps_components:
                    self.db.insert_calculated_eps(
                        ticker,
                        eps_components['eps_growth_last_qtr'],
                        eps_components['eps_growth_prev_qtr'],
                        eps_components['annual_growth_rate'],
                        eps_components['stability_score']
                    )
                    success_count += 1
            except Exception as e:
                continue

        print(f"  {success_count} 銘柄のEPS要素を計算しました\n")

    def calculate_eps_components(self, income_statements_quarterly: List[Dict],
                                 income_statements_annual: List[Dict] = None) -> Optional[Dict]:
        """
        EPS要素を計算（4つの独立した要素）

        Returns:
            dict: {
                'eps_growth_last_qtr': float,
                'eps_growth_prev_qtr': float,
                'annual_growth_rate': float,
                'stability_score': float
            }
        """
        if not income_statements_quarterly or len(income_statements_quarterly) < 5:
            return None

        try:
            result = {}

            # 1. 最新四半期のEPS成長率（前年同期比）
            latest_eps = income_statements_quarterly[0].get('eps', 0)
            yoy_eps_q0 = income_statements_quarterly[4].get('eps', 0) if len(income_statements_quarterly) > 4 else 0

            if yoy_eps_q0 and yoy_eps_q0 != 0 and latest_eps is not None:
                eps_growth_last_qtr = ((latest_eps - yoy_eps_q0) / abs(yoy_eps_q0)) * 100
            else:
                eps_growth_last_qtr = None

            result['eps_growth_last_qtr'] = eps_growth_last_qtr

            # 2. 前四半期のEPS成長率（前年同期比）
            if len(income_statements_quarterly) >= 6:
                prev_qtr_eps = income_statements_quarterly[1].get('eps', 0)
                yoy_eps_q1 = income_statements_quarterly[5].get('eps', 0)

                if yoy_eps_q1 and yoy_eps_q1 != 0 and prev_qtr_eps is not None:
                    eps_growth_prev_qtr = ((prev_qtr_eps - yoy_eps_q1) / abs(yoy_eps_q1)) * 100
                else:
                    eps_growth_prev_qtr = None

                result['eps_growth_prev_qtr'] = eps_growth_prev_qtr
            else:
                result['eps_growth_prev_qtr'] = None

            # 3. 年間EPS成長率（3年CAGR）
            annual_growth_rate = None
            if income_statements_annual and len(income_statements_annual) >= 3:
                try:
                    eps_values = [stmt.get('eps', 0) for stmt in income_statements_annual[:3]]
                    if eps_values[0] and eps_values[0] > 0 and eps_values[-1] and eps_values[-1] > 0:
                        years = len(eps_values) - 1
                        cagr = (pow(eps_values[0] / eps_values[-1], 1/years) - 1) * 100
                        annual_growth_rate = cagr
                except:
                    pass

            result['annual_growth_rate'] = annual_growth_rate

            # 4. 収益安定性スコア（変動係数）
            stability_score = None
            if len(income_statements_quarterly) >= 8:
                try:
                    eps_last_8q = [stmt.get('eps', 0) for stmt in income_statements_quarterly[:8]]
                    eps_last_8q = [e for e in eps_last_8q if e is not None and e > 0]

                    if len(eps_last_8q) >= 6:
                        eps_mean = np.mean(eps_last_8q)
                        eps_std = np.std(eps_last_8q)

                        if eps_mean > 0:
                            coefficient_of_variation = eps_std / eps_mean
                            # スコアに変換（0-100、低いCVほど高スコア）
                            # CV=0 -> 100点, CV=1 -> 0点
                            stability_score = max(0, 100 - (coefficient_of_variation * 100))
                except:
                    pass

            result['stability_score'] = stability_score

            return result

        except Exception as e:
            return None

    # ==================== セクターパフォーマンスデータ収集 ====================

    def collect_sector_performance_data(self, limit: int = 300):
        """
        セクターパフォーマンスデータを収集してDBに保存

        Args:
            limit: 取得する履歴データの件数
        """
        print(f"\nセクターパフォーマンスデータを収集中...")

        # 履歴セクターパフォーマンスを取得
        historical_data = self.get_historical_sector_performance(limit=limit)

        if not historical_data:
            print("  セクターパフォーマンスデータの取得に失敗しました")
            return

        print(f"  {len(historical_data)} 件の履歴データを取得")

        # データを変換してDBに保存
        records = []
        for entry in historical_data:
            date = entry.get('date')
            if not date:
                continue

            # 各セクターのパフォーマンスを抽出
            for key, value in entry.items():
                if key != 'date' and value is not None:
                    # セクター名を正規化（例: "Communication Services" など）
                    sector_name = key.replace('ChangesPercentage', '').strip()
                    records.append({
                        'sector': sector_name,
                        'date': date,
                        'change_percentage': float(value) if isinstance(value, (int, float, str)) else None
                    })

        if records:
            # 重複を除去してDBに挿入
            unique_records = []
            seen = set()
            for record in records:
                key = (record['sector'], record['date'])
                if key not in seen and record['change_percentage'] is not None:
                    seen.add(key)
                    unique_records.append(record)

            self.db.insert_sector_performance_bulk(unique_records)
            print(f"  {len(unique_records)} 件のセクターパフォーマンスデータを保存しました")
        else:
            print("  保存するデータがありませんでした")

    # ==================== ベンチマークデータ収集 ====================

    def collect_benchmark_data(self, benchmark_tickers: List[str] = None):
        """
        ベンチマークティッカー（SPY、QQQなど）のデータを収集

        Args:
            benchmark_tickers: ベンチマークティッカーのリスト（デフォルト: ['SPY', 'QQQ', 'DIA']）
        """
        if benchmark_tickers is None:
            benchmark_tickers = ['SPY', 'QQQ', 'DIA']

        print(f"\n{'='*80}")
        print(f"ベンチマークデータを収集中: {', '.join(benchmark_tickers)}")
        print(f"{'='*80}")

        db_conn = IBDDatabase(self.db_path, silent=True)
        success_count = 0

        for ticker in benchmark_tickers:
            try:
                print(f"  {ticker} のデータを取得中...")

                # 株価データ取得（300日分）
                prices_df = self.get_historical_prices(ticker, days=300)
                if prices_df is not None and len(prices_df) >= 30:
                    db_conn.insert_price_history(ticker, prices_df)
                    print(f"    ✓ {ticker}: {len(prices_df)}日分のデータを保存")
                    success_count += 1
                else:
                    print(f"    ✗ {ticker}: データ取得失敗")

            except Exception as e:
                if self.debug:
                    print(f"    ✗ {ticker}: エラー - {str(e)}")

        db_conn.close()

        print(f"\nベンチマークデータ収集完了: {success_count}/{len(benchmark_tickers)} 成功")
        print(f"{'='*80}\n")

        return success_count

    # ==================== メインワークフロー ====================

    def run_full_collection(self, use_full_dataset: bool = True, max_workers: int = 3):
        """
        完全なデータ収集ワークフローを実行

        1. ベンチマークデータ収集（SPY、QQQなど）
        2. ティッカーリスト取得
        3. 全データ収集（株価、EPS、プロファイル）
        4. RS値計算
        5. EPS要素計算

        Args:
            use_full_dataset: 全銘柄を処理するか（Falseの場合は500銘柄に制限）
            max_workers: 並列処理のワーカー数（デフォルト3: 750 calls/min制限に対応）
        """
        # 1. ベンチマークデータ収集（最優先）
        self.collect_benchmark_data()

        # 2. ティッカーリスト取得
        print("\nティッカーリストを取得中...")
        fetcher = FMPTickerFetcher()
        tickers_df = fetcher.get_all_stocks(['nasdaq', 'nyse'])
        tickers_list = tickers_df['Ticker'].tolist()
        print(f"  合計 {len(tickers_list)} 銘柄を取得しました")

        # テスト用にサンプルサイズを制限
        if not use_full_dataset:
            sample_size = min(500, len(tickers_list))
            tickers_list = tickers_list[:sample_size]
            print(f"  テストモード: {sample_size} 銘柄に制限")

        # 3. データ収集
        collected_tickers = self.collect_all_data(tickers_list, max_workers=max_workers)

        # 4. セクターパフォーマンスデータ収集
        self.collect_sector_performance_data(limit=300)

        # 5. RS値計算
        self.calculate_and_store_rs_values(collected_tickers)

        # 6. EPS要素計算
        self.calculate_and_store_eps_components(collected_tickers)

        # 7. 統計表示
        self.db.get_database_stats()

        print(f"\n{'='*80}")
        print("全データ収集完了!")
        print(f"{'='*80}\n")

    def close(self):
        """リソースをクリーンアップ"""
        self.db.close()


def main():
    """テスト実行"""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    FMP_API_KEY = os.getenv('FMP_API_KEY')
    if not FMP_API_KEY or FMP_API_KEY == 'your_api_key_here':
        print("エラー: FMP_API_KEYが設定されていません")
        return

    try:
        collector = IBDDataCollector(FMP_API_KEY)

        # テストモードで実行（500銘柄）
        collector.run_full_collection(use_full_dataset=False, max_workers=3)

        collector.close()

    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
