"""
IBD Screeners

データベースに保存された計算済みレーティングを使用してスクリーナーを実行します。
"""

import gspread
import numpy as np
from typing import List, Dict, Optional
from google.oauth2.service_account import Credentials

from ibd_database import IBDDatabase


class IBDScreeners:
    """データベースを使用したIBDスクリーナー"""

    def __init__(self, credentials_file: str, spreadsheet_name: str, db_path: str = 'ibd_data.db',
                 drive_folder_id: str = None):
        """
        Args:
            credentials_file: Googleサービスアカウントの認証情報JSONファイルパス
            spreadsheet_name: Googleスプレッドシートの名前
            db_path: データベースファイルのパス
            drive_folder_id: Google Driveの共有フォルダID（画像アップロード用、オプション）
        """
        self.db = IBDDatabase(db_path)
        self.drive_folder_id = drive_folder_id

        # Google Sheets認証
        try:
            self.gc = gspread.service_account(filename=credentials_file)
            # Google Sheets API用の認証情報を別途読み込む
            self.credentials = Credentials.from_service_account_file(
                credentials_file,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
        except FileNotFoundError:
            print(f"エラー: 認証情報ファイル '{credentials_file}' が見つかりません")
            raise

        # スプレッドシートを開く（存在しない場合は作成）
        try:
            self.spreadsheet = self.gc.open(spreadsheet_name)
        except gspread.SpreadsheetNotFound:
            self.spreadsheet = self.gc.create(spreadsheet_name)
            self.spreadsheet.share('', perm_type='anyone', role='reader')
            print(f"新しいスプレッドシート '{spreadsheet_name}' を作成しました")

    def close(self):
        """リソースをクリーンアップ"""
        self.db.close()

    # ==================== ヘルパーメソッド ====================

    def get_price_metrics(self, ticker: str) -> Optional[Dict]:
        """価格関連の指標を計算"""
        prices_df = self.db.get_price_history(ticker, days=180)
        if prices_df is None or len(prices_df) < 2:
            return None

        try:
            close = prices_df['close'].values
            open_price = prices_df['open'].values

            result = {
                'price': close[-1],
                'pct_change_1d': ((close[-1] - close[-2]) / close[-2] * 100) if close[-2] != 0 else 0,
                'change_from_open': ((close[-1] - open_price[-1]) / open_price[-1] * 100) if open_price[-1] != 0 else 0,
                'pct_1m': ((close[-1] - close[-21]) / close[-21] * 100) if len(close) >= 21 and close[-21] != 0 else None,
                'pct_3m': ((close[-1] - close[-63]) / close[-63] * 100) if len(close) >= 63 and close[-63] != 0 else None,
                'pct_6m': ((close[-1] - close[-126]) / close[-126] * 100) if len(close) >= 126 and close[-126] != 0 else None
            }
            return result
        except Exception as e:
            return None

    def get_volume_metrics(self, ticker: str) -> Optional[Dict]:
        """ボリューム関連の指標を計算"""
        prices_df = self.db.get_price_history(ticker, days=100)
        if prices_df is None or len(prices_df) < 90:
            return None

        try:
            volume = prices_df['volume'].values

            avg_volume_50 = np.mean(volume[-50:]) if len(volume) >= 50 else None
            avg_volume_90 = np.mean(volume[-90:]) if len(volume) >= 90 else None
            current_volume = volume[-1]

            vol_change_pct = ((current_volume - avg_volume_50) / avg_volume_50 * 100) if avg_volume_50 and avg_volume_50 > 0 else 0
            rel_volume = (current_volume / avg_volume_50) if avg_volume_50 and avg_volume_50 > 0 else 0

            return {
                'avg_vol_50': avg_volume_50 / 1000,
                'avg_vol_90': avg_volume_90 / 1000,
                'current_volume': current_volume / 1000,
                'vol_change_pct': vol_change_pct,
                'rel_volume': rel_volume
            }
        except Exception as e:
            return None

    def get_moving_averages(self, ticker: str) -> Optional[Dict]:
        """移動平均を計算"""
        prices_df = self.db.get_price_history(ticker, days=250)
        if prices_df is None or len(prices_df) < 200:
            return None

        try:
            close = prices_df['close'].values

            return {
                '10ma': np.mean(close[-10:]) if len(close) >= 10 else None,
                '21ma': np.mean(close[-21:]) if len(close) >= 21 else None,
                '50ma': np.mean(close[-50:]) if len(close) >= 50 else None,
                '150ma': np.mean(close[-150:]) if len(close) >= 150 else None,
                '200ma': np.mean(close[-200:]) if len(close) >= 200 else None,
                'price': close[-1]
            }
        except Exception as e:
            return None

    def get_price_vs_50ma(self, ticker: str) -> Optional[float]:
        """価格と50日移動平均の比較"""
        ma_data = self.get_moving_averages(ticker)
        if not ma_data or ma_data['50ma'] is None:
            return None

        try:
            return ((ma_data['price'] - ma_data['50ma']) / ma_data['50ma'] * 100)
        except:
            return None

    def calculate_relative_strength(self, benchmark_prices, target_prices, days=25):
        """
        相対強度(RS)を計算

        Args:
            benchmark_prices: ベンチマークの価格データ（DataFrame）
            target_prices: ターゲット銘柄の価格データ（DataFrame）
            days: 使用する日数

        Returns:
            np.array: 日次のRS比率の配列
        """
        if benchmark_prices is None or target_prices is None:
            return None

        # 日付でマージして共通の日付のみを使用（重要：日付の不一致を防ぐ）
        import pandas as pd
        merged = pd.merge(
            benchmark_prices[['date', 'close']].rename(columns={'close': 'benchmark_close'}),
            target_prices[['date', 'close']].rename(columns={'close': 'target_close'}),
            on='date',
            how='inner'
        )

        if len(merged) == 0:
            return None

        # 最新のdays日分を使用
        if len(merged) > days:
            merged = merged.tail(days)

        # データが不足している場合
        if len(merged) < days:
            return None

        # ゼロ除算を防ぐ
        if (merged['benchmark_close'] == 0).any():
            return None

        rs = merged['target_close'].values / merged['benchmark_close'].values
        return rs

    def calculate_rs_sts_percentile(self, rs_values):
        """
        RS STS % (パーセンタイル)を計算

        Args:
            rs_values: RS値の配列

        Returns:
            float: パーセンタイル（0-100）
        """
        if rs_values is None or len(rs_values) == 0:
            return 0

        latest_rs = rs_values[-1]
        percentile = (np.sum(rs_values <= latest_rs) / len(rs_values)) * 100

        return round(percentile, 2)

    def get_rs_sts_percentile(self, ticker: str, benchmark_ticker: str = 'SPY', debug: bool = False) -> Optional[float]:
        """
        指定銘柄のRS STS%を計算

        Args:
            ticker: ターゲット銘柄
            benchmark_ticker: ベンチマーク銘柄（デフォルト: SPY）
            debug: デバッグ情報を出力するか

        Returns:
            float: RS STS%（0-100）、計算できない場合はNone
        """
        # ベンチマークと銘柄の価格データを取得
        benchmark_prices = self.db.get_price_history(benchmark_ticker, days=30)
        ticker_prices = self.db.get_price_history(ticker, days=30)

        if benchmark_prices is None:
            if debug:
                print(f"    DEBUG: {ticker} - Benchmark ({benchmark_ticker}) price data is None")
            return None

        if ticker_prices is None:
            if debug:
                print(f"    DEBUG: {ticker} - Ticker price data is None")
            return None

        if len(benchmark_prices) < 25 or len(ticker_prices) < 25:
            if debug:
                print(f"    DEBUG: {ticker} - Insufficient data (benchmark: {len(benchmark_prices)}, ticker: {len(ticker_prices)})")
            return None

        # RSを計算（全データを渡して、calculate_relative_strength内で日付マージ後に25日分を使用）
        rs_values = self.calculate_relative_strength(
            benchmark_prices,
            ticker_prices,
            days=25
        )

        if rs_values is None:
            return None

        # RS STS%を計算
        return self.calculate_rs_sts_percentile(rs_values)

    def check_rs_line_new_high(self, ticker: str) -> bool:
        """RS Lineが新高値かチェック"""
        # 簡易版: 52週高値に近いかで判定
        rating = self.db.get_rating(ticker)
        if not rating or rating['price_vs_52w_high'] is None:
            return False

        # 52週高値から5%以内
        return rating['price_vs_52w_high'] >= -5

    # ==================== スクリーナー実装 ====================

    def screener_momentum_97(self) -> List[str]:
        """
        Momentum 97 スクリーナー

        条件:
        - 1M Rank (Pct) ≥ 97%
        - 3M Rank (Pct) ≥ 97%
        - 6M Rank (Pct) ≥ 97%
        """
        print("\n=== Momentum 97 スクリーナー実行中 ===")

        tickers_list = self.db.get_all_tickers()
        performance_data = {}

        # 全銘柄のパフォーマンスを取得
        for ticker in tickers_list:
            price_metrics = self.get_price_metrics(ticker)
            if price_metrics and price_metrics['pct_1m'] is not None and price_metrics['pct_3m'] is not None and price_metrics['pct_6m'] is not None:
                performance_data[ticker] = {
                    '1m': price_metrics['pct_1m'],
                    '3m': price_metrics['pct_3m'],
                    '6m': price_metrics['pct_6m']
                }

        # 各期間でパーセンタイルランクを計算
        def calc_percentile_ranks(values_dict, key):
            valid = {t: v[key] for t, v in values_dict.items() if v[key] is not None}
            if not valid:
                return {}
            sorted_items = sorted(valid.items(), key=lambda x: x[1])
            total = len(sorted_items)
            return {t: ((idx + 1) / total) * 100 for idx, (t, v) in enumerate(sorted_items)}

        rank_1m = calc_percentile_ranks(performance_data, '1m')
        rank_3m = calc_percentile_ranks(performance_data, '3m')
        rank_6m = calc_percentile_ranks(performance_data, '6m')

        # フィルタリング
        passed = []
        for ticker in performance_data.keys():
            if (rank_1m.get(ticker, 0) >= 97 and
                rank_3m.get(ticker, 0) >= 97 and
                rank_6m.get(ticker, 0) >= 97):
                passed.append(ticker)

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_explosive_eps_growth(self) -> List[str]:
        """
        Explosive Estimated EPS Growth Stocks スクリーナー

        条件:
        - RS Rating ≥ 80
        - RS STS% ≥ 80
        - EPS Growth Last Qtr ≥ 100%
        - 50-Day Avg Vol ≥ 100K
        - Price vs 50-Day ≥ 0.0%
        """
        print("\n=== Explosive Estimated EPS Growth Stocks スクリーナー実行中 ===")

        passed = []
        all_ratings = self.db.get_all_ratings()

        # デバッグ: 最初の銘柄でRS STS%をテスト
        import os
        debug_mode = os.getenv('IBD_DEBUG', 'false').lower() == 'true'
        if debug_mode and len(all_ratings) > 0:
            test_ticker = list(all_ratings.keys())[0]
            print(f"\n  DEBUG: Testing RS STS% calculation for {test_ticker}")
            test_rs_sts = self.get_rs_sts_percentile(test_ticker, debug=True)
            print(f"  DEBUG: Result: {test_rs_sts}\n")

        for ticker, rating in all_ratings.items():
            try:
                # RS Rating チェック
                if rating['rs_rating'] is None or rating['rs_rating'] < 80:
                    continue

                # RS STS% チェック
                rs_sts = self.get_rs_sts_percentile(ticker)
                if rs_sts is None or rs_sts < 80:
                    continue

                # EPS Growth チェック
                eps_components = self.db.get_all_eps_components()
                if ticker not in eps_components:
                    continue

                eps_growth = eps_components[ticker]['eps_growth_last_qtr']
                if eps_growth is None or eps_growth < 100:
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics or vol_metrics['avg_vol_50'] < 100:
                    continue

                # Price vs 50-Day MA チェック
                price_vs_50ma = self.get_price_vs_50ma(ticker)
                if price_vs_50ma is None or price_vs_50ma < 0:
                    continue

                passed.append(ticker)
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_up_on_volume(self) -> List[str]:
        """
        Up on Volume List スクリーナー

        条件:
        - Price % Chg ≥ 0.00%
        - Vol% Chg vs 50-Day ≥ 20%
        - Current Price ≥ $10
        - 50-Day Avg Vol ≥ 100K
        - Market Cap ≥ $250M
        - RS Rating ≥ 80
        - RS STS% ≥ 80
        - EPS % Chg Last Qtr ≥ 20%
        - A/D Rating ABC
        """
        print("\n=== Up on Volume List スクリーナー実行中 ===")

        passed = []
        all_ratings = self.db.get_all_ratings()

        for ticker, rating in all_ratings.items():
            try:
                # RS Rating チェック
                if rating['rs_rating'] is None or rating['rs_rating'] < 80:
                    continue

                # RS STS% チェック
                rs_sts = self.get_rs_sts_percentile(ticker)
                if rs_sts is None or rs_sts < 80:
                    continue

                # A/D Rating チェック
                if rating['ad_rating'] not in ['A', 'B', 'C']:
                    continue

                # 価格チェック
                price_metrics = self.get_price_metrics(ticker)
                if not price_metrics:
                    continue

                if price_metrics['pct_change_1d'] < 0:
                    continue

                if price_metrics['price'] < 10:
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics:
                    continue

                if vol_metrics['avg_vol_50'] < 100:
                    continue

                if vol_metrics['vol_change_pct'] < 20:
                    continue

                # 時価総額チェック
                profile = self.db.get_company_profile(ticker)
                if not profile or profile['market_cap'] is None:
                    continue

                market_cap_millions = profile['market_cap'] / 1_000_000
                if market_cap_millions < 250:
                    continue

                # EPS成長率チェック
                eps_components = self.db.get_all_eps_components()
                if ticker not in eps_components:
                    continue

                eps_growth = eps_components[ticker]['eps_growth_last_qtr']
                if eps_growth is None or eps_growth < 20:
                    continue

                passed.append(ticker)
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_top_2_percent_rs(self) -> List[str]:
        """
        Top 2% RS Rating List スクリーナー

        条件:
        - RS Rating ≥ 98
        - RS STS% ≥ 80
        - 10Day > 21Day > 50Day
        - 50-Day Avg Vol ≥ 100K
        - Volume ≥ 100K
        - Sector NOT: medical/healthcare
        """
        print("\n=== Top 2% RS Rating List スクリーナー実行中 ===")

        passed = []
        all_ratings = self.db.get_all_ratings()

        for ticker, rating in all_ratings.items():
            try:
                # RS Rating チェック
                if rating['rs_rating'] is None or rating['rs_rating'] < 98:
                    continue

                # RS STS% チェック
                rs_sts = self.get_rs_sts_percentile(ticker)
                if rs_sts is None or rs_sts < 80:
                    continue

                # 移動平均チェック
                ma_data = self.get_moving_averages(ticker)
                if not ma_data:
                    continue

                if not (ma_data['10ma'] > ma_data['21ma'] > ma_data['50ma']):
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics:
                    continue

                if vol_metrics['avg_vol_50'] < 100:
                    continue

                if vol_metrics['current_volume'] < 100:
                    continue

                # セクターチェック
                profile = self.db.get_company_profile(ticker)
                if profile:
                    sector = profile.get('sector', '').lower()
                    if 'healthcare' in sector or 'medical' in sector:
                        continue

                passed.append(ticker)
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_4_percent_bullish_yesterday(self) -> List[str]:
        """
        4% Bullish Yesterday スクリーナー

        条件:
        - Price ≥ $1
        - Change > 4%
        - Market cap > $250M
        - Volume > 100K
        - Rel Volume > 1
        - Change from Open > 0%
        - Avg Volume 90D > 100K
        - RS STS% ≥ 80
        """
        print("\n=== 4% Bullish Yesterday スクリーナー実行中 ===")

        passed = []
        tickers_list = self.db.get_all_tickers()

        for ticker in tickers_list:
            try:
                # 価格チェック
                price_metrics = self.get_price_metrics(ticker)
                if not price_metrics:
                    continue

                if price_metrics['price'] < 1:
                    continue

                if price_metrics['pct_change_1d'] <= 4:
                    continue

                if price_metrics['change_from_open'] <= 0:
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics:
                    continue

                if vol_metrics['current_volume'] <= 100:
                    continue

                if vol_metrics['rel_volume'] <= 1:
                    continue

                if vol_metrics['avg_vol_90'] <= 100:
                    continue

                # 時価総額チェック
                profile = self.db.get_company_profile(ticker)
                if not profile or profile['market_cap'] is None:
                    continue

                market_cap_millions = profile['market_cap'] / 1_000_000
                if market_cap_millions <= 250:
                    continue

                # RS STS% チェック
                rs_sts = self.get_rs_sts_percentile(ticker)
                if rs_sts is None or rs_sts < 80:
                    continue

                passed.append(ticker)
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_healthy_chart_watchlist(self) -> List[str]:
        """
        Healthy Chart Watch List スクリーナー

        条件:
        - 10Day > 21Day > 50Day
        - 50Day > 150Day > 200Day
        - RS Line New High
        - RS Rating ≥ 90
        - A/D Rating AB
        - Comp Rating ≥ 80
        - 50-Day Avg Vol ≥ 100K
        """
        print("\n=== Healthy Chart Watch List スクリーナー実行中 ===")

        passed = []
        all_ratings = self.db.get_all_ratings()

        for ticker, rating in all_ratings.items():
            try:
                # RS Rating チェック
                if rating['rs_rating'] is None or rating['rs_rating'] < 90:
                    continue

                # Composite Rating チェック
                if rating['comp_rating'] is None or rating['comp_rating'] < 80:
                    continue

                # A/D Rating チェック
                if rating['ad_rating'] not in ['A', 'B']:
                    continue

                # 移動平均チェック
                ma_data = self.get_moving_averages(ticker)
                if not ma_data:
                    continue

                if not (ma_data['10ma'] > ma_data['21ma'] > ma_data['50ma']):
                    continue

                if not (ma_data['50ma'] > ma_data['150ma'] > ma_data['200ma']):
                    continue

                # RS Line New High チェック
                if not self.check_rs_line_new_high(ticker):
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics or vol_metrics['avg_vol_50'] < 100:
                    continue

                passed.append(ticker)
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    # ==================== メイン実行関数 ====================

    def ensure_benchmark_data(self, benchmark_ticker: str = 'SPY'):
        """
        ベンチマークデータが存在することを確認し、なければ取得

        Args:
            benchmark_ticker: ベンチマークティッカー（デフォルト: SPY）

        Returns:
            bool: データが存在するか
        """
        # データベースにデータが存在するか確認
        benchmark_prices = self.db.get_price_history(benchmark_ticker, days=30)

        if benchmark_prices is not None and len(benchmark_prices) >= 25:
            print(f"✓ {benchmark_ticker} データは既に存在します ({len(benchmark_prices)}日分)")
            return True

        # データが存在しない場合、取得を試みる
        print(f"\n⚠ {benchmark_ticker} データが存在しません。取得中...")

        try:
            import os
            from dotenv import load_dotenv
            from ibd_data_collector import IBDDataCollector

            load_dotenv()
            fmp_api_key = os.getenv('FMP_API_KEY')

            if not fmp_api_key or fmp_api_key == 'your_api_key_here':
                print(f"✗ エラー: FMP_API_KEYが設定されていません")
                print(f"  RS STS%の計算には{benchmark_ticker}のデータが必須です")
                return False

            collector = IBDDataCollector(fmp_api_key, db_path=self.db.db_path)
            success = collector.collect_benchmark_data([benchmark_ticker])
            collector.close()

            if success > 0:
                print(f"✓ {benchmark_ticker} データの取得に成功しました")
                return True
            else:
                print(f"✗ {benchmark_ticker} データの取得に失敗しました")
                return False

        except Exception as e:
            print(f"✗ ベンチマークデータ取得エラー: {str(e)}")
            return False

    def run_all_screeners(self):
        """全スクリーナーを実行してGoogleスプレッドシートに出力"""
        print("\n" + "="*80)
        print("IBD スクリーナー実行開始")
        print("="*80)

        # ベンチマークデータの確認・取得
        print("\nベンチマークデータを確認中...")
        if not self.ensure_benchmark_data('SPY'):
            print("\n⚠ 警告: SPYデータが取得できませんでした")
            print("  RS STS%を使用するスクリーナーの結果が制限される可能性があります")
            print("  続行しますか? (Ctrl+C で中止)")
            import time
            time.sleep(3)

        # 各スクリーナーを実行
        screener_results = {}

        screener_results['Momentum 97'] = self.screener_momentum_97()
        screener_results['Explosive Estimated EPS Growth Stocks'] = self.screener_explosive_eps_growth()
        screener_results['Up on Volume List'] = self.screener_up_on_volume()
        screener_results['Top 2% RS Rating List'] = self.screener_top_2_percent_rs()
        screener_results['4% Bullish Yesterday'] = self.screener_4_percent_bullish_yesterday()
        screener_results['Healthy Chart Watch List'] = self.screener_healthy_chart_watchlist()

        # Googleスプレッドシートに出力
        print("\nGoogleスプレッドシートに出力中...")
        self.write_screeners_to_sheet(screener_results)

        print("\n" + "="*80)
        print("すべてのスクリーナー実行完了!")
        print(f"スプレッドシートURL: {self.spreadsheet.url}")
        print("="*80)

    def _get_industry_group_quadrant(self, ticker: str, sector_rotation_df) -> str:
        """
        ティッカーのIndustry Groupがどの象限にあるかを判定

        Args:
            ticker: ティッカーシンボル
            sector_rotation_df: セクターローテーションデータ

        Returns:
            str: 'Strong', 'Improving', 'Weakening', 'Weak', または None
        """
        if sector_rotation_df is None or len(sector_rotation_df) == 0:
            return None

        # ティッカーのIndustry Groupを取得
        profile = self.db.get_company_profile(ticker)
        if not profile or not profile.get('industry'):
            return None

        industry = profile['industry']

        # セクターローテーションデータから該当するIndustry Groupを検索
        industry_data = sector_rotation_df[sector_rotation_df['industry'] == industry]

        if len(industry_data) == 0:
            return None

        # 週次・月次RSを取得
        weekly_rs = industry_data.iloc[0]['weekly_rs']
        monthly_rs = industry_data.iloc[0]['monthly_rs']

        # パーセンタイルランキングに変換（0-100）
        weekly_percentile = (sector_rotation_df['weekly_rs'] <= weekly_rs).sum() / len(sector_rotation_df) * 100
        monthly_percentile = (sector_rotation_df['monthly_rs'] <= monthly_rs).sum() / len(sector_rotation_df) * 100

        # 象限を判定
        if weekly_percentile >= 50 and monthly_percentile >= 50:
            return 'Strong'
        elif weekly_percentile >= 50 and monthly_percentile < 50:
            return 'Improving'
        elif weekly_percentile < 50 and monthly_percentile >= 50:
            return 'Weakening'
        else:
            return 'Weak'

    def _get_quadrant_color(self, quadrant: str) -> dict:
        """
        象限に対応する背景色を取得

        Args:
            quadrant: 'Strong', 'Improving', 'Weakening', 'Weak'

        Returns:
            dict: Google Sheets APIのbackgroundColor形式
        """
        colors = {
            'Strong': {'red': 0.785, 'green': 0.902, 'blue': 0.788},      # #c8e6c9
            'Improving': {'red': 0.910, 'green': 0.961, 'blue': 0.914},   # #e8f5e9
            'Weakening': {'red': 1.0, 'green': 0.922, 'blue': 0.933},     # #ffebee
            'Weak': {'red': 1.0, 'green': 0.804, 'blue': 0.824}           # #ffcdd2
        }
        return colors.get(quadrant, {'red': 1, 'green': 1, 'blue': 1})  # デフォルトは白

    def _retry_api_call(self, func, max_retries=4, initial_delay=2):
        """
        API呼び出しをリトライロジック付きで実行

        Args:
            func: 実行する関数
            max_retries: 最大リトライ回数
            initial_delay: 初期待機時間（秒）

        Returns:
            関数の実行結果
        """
        import time
        from gspread.exceptions import APIError

        delay = initial_delay
        for attempt in range(max_retries):
            try:
                return func()
            except APIError as e:
                if '429' in str(e) and attempt < max_retries - 1:
                    print(f"    レート制限エラー（429）が発生しました。{delay}秒後にリトライします... (試行 {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    delay *= 2  # 指数バックオフ
                else:
                    raise

        # すべてのリトライが失敗した場合
        raise Exception(f"API呼び出しが{max_retries}回のリトライ後も失敗しました")

    def write_screeners_to_sheet(self, screener_results: Dict[str, List[str]]):
        """スクリーナー結果をGoogleスプレッドシートに出力（最適化版）"""
        import time
        from googleapiclient.discovery import build

        # データベースから最新の価格データ日付を取得してシート名とする
        latest_date = self.db.get_latest_price_date()
        sheet_name = latest_date if latest_date else 'IBD Screeners'

        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            self._retry_api_call(lambda: worksheet.clear())
            time.sleep(1)
        except gspread.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(
                title=sheet_name,
                rows=1000,
                cols=10
            )
            time.sleep(1)

        # Google Sheets API v4 サービスを構築
        sheets_service = build('sheets', 'v4', credentials=self.credentials)

        # セクターローテーションデータを取得
        print("\nIndustry Group象限データを読み込み中...")
        sector_rotation_df = self.db.get_sector_rotation_data()

        current_row = 1
        all_format_requests = []  # すべてのフォーマットリクエストを収集

        for screener_name, tickers in screener_results.items():
            # スクリーナー名を出力
            header_row = current_row
            self._retry_api_call(lambda row=header_row, name=screener_name: worksheet.update(f'A{row}', [[name]]))
            time.sleep(0.5)

            # ヘッダー行のフォーマットリクエストを追加
            all_format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': header_row - 1,
                        'endRowIndex': header_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 10
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.6},
                            'textFormat': {
                                'bold': True,
                                'foregroundColor': {'red': 1, 'green': 1, 'blue': 1},
                                'fontSize': 12
                            },
                            'horizontalAlignment': 'LEFT'
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                }
            })

            # セルをマージ
            all_format_requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': header_row - 1,
                        'endRowIndex': header_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 10
                    },
                    'mergeType': 'MERGE_ALL'
                }
            })

            current_row += 1

            # ティッカーを10個ずつ横に並べる
            if tickers:
                rows_data = []
                for i in range(0, len(tickers), 10):
                    row_tickers = tickers[i:i+10]
                    while len(row_tickers) < 10:
                        row_tickers.append('')
                    rows_data.append(row_tickers)

                if rows_data:
                    start_row = current_row
                    end_row = start_row + len(rows_data) - 1
                    self._retry_api_call(lambda s=start_row, e=end_row, data=rows_data:
                                        worksheet.update(f'A{s}:J{e}', data))
                    time.sleep(1)

                    # 各ティッカーセルの背景色リクエストを追加
                    print(f"  {screener_name}: ティッカーの背景色設定を準備中...")
                    for row_idx, row_tickers in enumerate(rows_data):
                        for col_idx, ticker in enumerate(row_tickers):
                            if ticker:
                                quadrant = self._get_industry_group_quadrant(ticker, sector_rotation_df)
                                if quadrant:
                                    cell_row = current_row + row_idx
                                    color = self._get_quadrant_color(quadrant)

                                    # repeatCellリクエストを追加（単一セル）
                                    all_format_requests.append({
                                        'repeatCell': {
                                            'range': {
                                                'sheetId': worksheet.id,
                                                'startRowIndex': cell_row - 1,
                                                'endRowIndex': cell_row,
                                                'startColumnIndex': col_idx,
                                                'endColumnIndex': col_idx + 1
                                            },
                                            'cell': {
                                                'userEnteredFormat': {
                                                    'backgroundColor': color,
                                                    'textFormat': {'fontSize': 10},
                                                    'horizontalAlignment': 'CENTER'
                                                }
                                            },
                                            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                                        }
                                    })

                    current_row = end_row + 1

            current_row += 1

        # すべてのフォーマットリクエストを1回のbatchUpdateで実行
        if all_format_requests:
            print(f"\n背景色を一括適用中（{len(all_format_requests)}リクエスト）...")

            # バッチサイズを100に設定（Google Sheets APIの推奨値内で安全）
            batch_size = 100
            for i in range(0, len(all_format_requests), batch_size):
                batch = all_format_requests[i:i+batch_size]

                try:
                    request_body = {'requests': batch}
                    self._retry_api_call(
                        lambda body=request_body: sheets_service.spreadsheets().batchUpdate(
                            spreadsheetId=worksheet.spreadsheet.id,
                            body=body
                        ).execute()
                    )
                    print(f"  進捗: {min(i + batch_size, len(all_format_requests))}/{len(all_format_requests)} リクエスト完了")
                except Exception as e:
                    print(f"  警告: バッチフォーマットエラー ({str(e)})、スキップします")

                # レート制限回避のため待機（バッチ間のみ）
                if i + batch_size < len(all_format_requests):
                    time.sleep(2)

        # セクターローテーションチャートを生成
        print("\nセクターローテーションチャートを生成中...")
        from sector_rotation_chart import SectorRotationChart

        chart_generator = SectorRotationChart(db_path=self.db.db_path)
        chart_bytes = chart_generator.generate_chart_as_bytes(dpi=150)
        chart_generator.close()

        if chart_bytes:
            current_row += 2

            chart_title_row = current_row
            self._retry_api_call(lambda row=chart_title_row:
                                worksheet.update(f'A{row}', [['Industry Group RS Rotation Chart']]))
            time.sleep(1)

            # タイトルのフォーマット（batchUpdateで実行）
            title_format_request = {
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': chart_title_row - 1,
                        'endRowIndex': chart_title_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 10
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.1, 'green': 0.1, 'blue': 0.1},
                            'textFormat': {
                                'bold': True,
                                'foregroundColor': {'red': 1, 'green': 1, 'blue': 1},
                                'fontSize': 14
                            },
                            'horizontalAlignment': 'CENTER'
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                }
            }

            merge_request = {
                'mergeCells': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': chart_title_row - 1,
                        'endRowIndex': chart_title_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 10
                    },
                    'mergeType': 'MERGE_ALL'
                }
            }

            try:
                request_body = {'requests': [title_format_request, merge_request]}
                self._retry_api_call(
                    lambda body=request_body: sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=worksheet.spreadsheet.id,
                        body=body
                    ).execute()
                )
                time.sleep(1)
            except Exception as e:
                print(f"  タイトルフォーマットエラー: {str(e)}")

            current_row += 1

            # チャート画像を挿入
            try:
                image_url = self._upload_image_to_drive_and_insert(
                    worksheet, chart_bytes, current_row, 'sector_rotation_chart.png'
                )
                if image_url:
                    print(f"  チャートをGoogle Sheetsに挿入しました")
                else:
                    print(f"  チャート画像のアップロードに失敗しました")
            except Exception as e:
                print(f"  チャート挿入エラー: {str(e)}")
        else:
            print("  チャート生成に失敗しました")

        print(f"  '{sheet_name}' シートに出力完了")

    def _upload_image_to_drive_and_insert(self, worksheet, image_bytes: bytes,
                                           row: int, filename: str) -> str:
        """
        画像をGoogle Driveにアップロードし、Google Sheetsに=IMAGE()関数で挿入

        Args:
            worksheet: ワークシート
            image_bytes: 画像のバイトデータ
            row: 挿入する行番号（1始まり）
            filename: ファイル名

        Returns:
            str: アップロードされた画像のURL、失敗時はNone
        """
        try:
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaInMemoryUpload
            import tempfile
            import os
            import time

            # Google Drive APIサービスを構築
            # gspreadの認証情報を再利用
            drive_service = build('drive', 'v3', credentials=self.credentials)

            # 画像をGoogle Driveにアップロード
            file_metadata = {
                'name': filename,
                'mimeType': 'image/png'
            }

            # 共有フォルダIDが指定されている場合は使用、なければスプレッドシートの親フォルダを取得
            if self.drive_folder_id:
                file_metadata['parents'] = [self.drive_folder_id]
            else:
                # スプレッドシートの親フォルダIDを取得
                spreadsheet_id = worksheet.spreadsheet.id
                try:
                    file_metadata_query = drive_service.files().get(
                        fileId=spreadsheet_id,
                        fields='parents'
                    ).execute()
                    parent_folder_ids = file_metadata_query.get('parents', [])
                    if parent_folder_ids:
                        file_metadata['parents'] = parent_folder_ids
                except Exception as e:
                    print(f"  警告: スプレッドシートの親フォルダ取得に失敗: {str(e)}")
                    # 親フォルダが取得できない場合はルートにアップロード（失敗する可能性あり）
                    pass

            media = MediaInMemoryUpload(image_bytes, mimetype='image/png', resumable=True)

            uploaded_file = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink, webContentLink'
            ).execute()

            file_id = uploaded_file.get('id')

            # ファイルを公開設定にする
            drive_service.permissions().create(
                fileId=file_id,
                body={'type': 'anyone', 'role': 'reader'}
            ).execute()

            # 画像の直接リンクを取得
            image_url = f"https://drive.google.com/uc?export=view&id={file_id}"

            # Google Sheetsに=IMAGE()関数を使って画像を挿入
            # セルの高さを調整して画像を表示
            image_formula = f'=IMAGE("{image_url}", 1)'
            self._retry_api_call(lambda r=row, formula=image_formula: worksheet.update(f'A{r}', [[formula]]))
            time.sleep(1)  # API呼び出し後に待機

            # セルのサイズを調整（行の高さを設定）
            # Google Sheets APIを使用して行の高さを設定
            try:
                sheets_service = build('sheets', 'v4', credentials=self.credentials)

                # 行の高さを600ピクセルに設定
                request_body = {
                    'requests': [
                        {
                            'updateDimensionProperties': {
                                'range': {
                                    'sheetId': worksheet.id,
                                    'dimension': 'ROWS',
                                    'startIndex': row - 1,  # 0始まり
                                    'endIndex': row
                                },
                                'properties': {
                                    'pixelSize': 600
                                },
                                'fields': 'pixelSize'
                            }
                        }
                    ]
                }

                def update_row_height():
                    return sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=worksheet.spreadsheet.id,
                        body=request_body
                    ).execute()

                self._retry_api_call(update_row_height)
                time.sleep(1)  # API呼び出し後に待機

            except Exception as e:
                print(f"  警告: 行の高さ調整に失敗: {str(e)}")

            return image_url

        except Exception as e:
            print(f"  画像アップロードエラー: {str(e)}")
            import traceback
            traceback.print_exc()
            return None


def main():
    """テスト実行"""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE', 'credentials.json')
    SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME', 'Market Dashboard')
    DRIVE_FOLDER_ID = os.getenv('GDRIVE_FOLDER_ID')  # オプション: 共有フォルダID

    if DRIVE_FOLDER_ID:
        print(f"画像アップロード用の共有フォルダID: {DRIVE_FOLDER_ID}")
    else:
        print("警告: GDRIVE_FOLDER_IDが設定されていません。画像アップロードはスキップされる可能性があります。")

    try:
        screeners = IBDScreeners(CREDENTIALS_FILE, SPREADSHEET_NAME, drive_folder_id=DRIVE_FOLDER_ID)
        screeners.run_all_screeners()
        screeners.close()

    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
