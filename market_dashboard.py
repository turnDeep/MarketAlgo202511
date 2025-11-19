import gspread
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

class MarketDashboard:
    def __init__(self, fmp_api_key, credentials_file, spreadsheet_name):
        """
        マーケットダッシュボードの初期化

        Args:
            fmp_api_key: Financial Modeling Prep API Key
            credentials_file: Googleサービスアカウントの認証情報JSONファイルパス
            spreadsheet_name: Googleスプレッドシートの名前
        """
        self.fmp_api_key = fmp_api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"

        # Google Sheets認証（最新のgspread APIを使用）
        try:
            self.gc = gspread.service_account(filename=credentials_file)
        except FileNotFoundError:
            print(f"エラー: 認証情報ファイル '{credentials_file}' が見つかりません")
            print("Google Cloud Platformでサービスアカウントを作成し、JSONキーをダウンロードしてください")
            raise

        # スプレッドシートを開く（存在しない場合は作成）
        try:
            self.spreadsheet = self.gc.open(spreadsheet_name)
        except gspread.SpreadsheetNotFound:
            self.spreadsheet = self.gc.create(spreadsheet_name)
            # スプレッドシートを共有可能にする（オプション）
            self.spreadsheet.share('', perm_type='anyone', role='reader')
            print(f"新しいスプレッドシート '{spreadsheet_name}' を作成しました")

    def get_historical_prices(self, symbol, days=None):
        """
        指定されたシンボルの過去価格データを取得

        Args:
            symbol: ティッカーシンボル
            days: 取得する営業日数（Noneの場合は可能な限り取得）

        Returns:
            pandas DataFrame: 日付、終値、その他のデータ
        """
        # 特殊なシンボルの処理
        original_symbol = symbol
        if symbol in ['VIX', '^VIX']:
            symbol = '^VIX'
        elif symbol == 'NYICDX':
            symbol = 'DX-Y.NYB'
        elif symbol == 'DX-Y.NYB':
            symbol = 'DX-Y.NYB'

        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {'apikey': self.fmp_api_key}

        if days:
            params['timeseries'] = days + 20  # 余裕を持って取得

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'historical' not in data or not data['historical']:
                print(f"警告: {original_symbol} のデータが取得できませんでした")
                return None

            # DataFrameに変換
            df = pd.DataFrame(data['historical'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)

            return df

        except requests.exceptions.RequestException as e:
            print(f"エラー: {original_symbol} のデータ取得に失敗 - {str(e)}")
            return None
        except Exception as e:
            print(f"エラー: {original_symbol} のデータ処理に失敗 - {str(e)}")
            return None

    def get_quote(self, symbol):
        """
        リアルタイムの株価情報を取得
        """
        url = f"{self.base_url}/quote/{symbol}"
        params = {'apikey': self.fmp_api_key}

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data and len(data) > 0:
                return data[0]
            return None

        except Exception as e:
            print(f"エラー: {symbol} の株価取得に失敗 - {str(e)}")
            return None

    def calculate_performance(self, prices_df, periods):
        """
        各期間のパフォーマンスを計算

        Args:
            prices_df: 価格データのDataFrame
            periods: 計算する期間のリスト（例: ['1W', '1M', '1Y', 'YTD']）

        Returns:
            dict: 各期間のパフォーマンス
        """
        if prices_df is None or len(prices_df) == 0:
            return {period: 0 for period in periods}

        current_price = prices_df['close'].iloc[-1]
        current_date = prices_df['date'].iloc[-1]

        performance = {}

        for period in periods:
            if period == 'YTD':
                # 年初からのパフォーマンス
                year_start = pd.Timestamp(f"{current_date.year}-01-01")
                ytd_data = prices_df[prices_df['date'] >= year_start]
                if len(ytd_data) > 0:
                    start_price = ytd_data['close'].iloc[0]
                    performance['YTD'] = ((current_price - start_price) / start_price) * 100
                else:
                    performance['YTD'] = 0

            elif period == '1W':
                # 1週間前（5-7営業日前）
                if len(prices_df) >= 7:
                    start_price = prices_df['close'].iloc[-7]
                    performance['1W'] = ((current_price - start_price) / start_price) * 100
                else:
                    performance['1W'] = 0

            elif period == '1M':
                # 1ヶ月前（約21営業日前）
                if len(prices_df) >= 25:
                    start_price = prices_df['close'].iloc[-25]
                    performance['1M'] = ((current_price - start_price) / start_price) * 100
                else:
                    performance['1M'] = 0

            elif period == '1Y':
                # 1年前（約252営業日前）
                if len(prices_df) >= 260:
                    start_price = prices_df['close'].iloc[-260]
                    performance['1Y'] = ((current_price - start_price) / start_price) * 100
                else:
                    performance['1Y'] = 0

        return performance

    def calculate_52w_high_distance(self, prices_df):
        """
        52週高値からの距離を計算

        Args:
            prices_df: 価格データのDataFrame

        Returns:
            float: 52週高値からのパーセンテージ
        """
        if prices_df is None or len(prices_df) == 0:
            return 0

        # 過去52週（約260営業日）のデータを取得
        recent_data = prices_df.tail(260)

        if len(recent_data) == 0:
            return 0

        high_52w = recent_data['high'].max()
        current_price = prices_df['close'].iloc[-1]

        distance = ((current_price - high_52w) / high_52w) * 100

        return distance

    def calculate_moving_averages(self, prices_df):
        """
        移動平均を計算

        Args:
            prices_df: 価格データのDataFrame

        Returns:
            dict: 各期間の移動平均とトレンド情報
        """
        if prices_df is None or len(prices_df) < 200:
            return {
                '10MA': None, '20MA': None, '50MA': None, '200MA': None,
                'price_vs_10ma': None, 'price_vs_20ma': None,
                'price_vs_50ma': None, 'price_vs_200ma': None,
                '20_vs_50': None, '50_vs_200': None
            }

        current_price = prices_df['close'].iloc[-1]

        # 移動平均の計算
        ma_10 = prices_df['close'].tail(10).mean() if len(prices_df) >= 10 else None
        ma_20 = prices_df['close'].tail(20).mean() if len(prices_df) >= 20 else None
        ma_50 = prices_df['close'].tail(50).mean() if len(prices_df) >= 50 else None
        ma_200 = prices_df['close'].tail(200).mean() if len(prices_df) >= 200 else None

        result = {
            '10MA': ma_10,
            '20MA': ma_20,
            '50MA': ma_50,
            '200MA': ma_200,
            'price_vs_10ma': current_price > ma_10 if ma_10 else None,
            'price_vs_20ma': current_price > ma_20 if ma_20 else None,
            'price_vs_50ma': current_price > ma_50 if ma_50 else None,
            'price_vs_200ma': current_price > ma_200 if ma_200 else None,
            '20_vs_50': ma_20 > ma_50 if (ma_20 and ma_50) else None,
            '50_vs_200': ma_50 > ma_200 if (ma_50 and ma_200) else None
        }

        return result

    def calculate_relative_strength(self, benchmark_prices, target_prices, days=25):
        """
        相対強度(RS)を計算
        """
        if benchmark_prices is None or target_prices is None:
            return None

        # 両方のデータフレームの長さを揃える
        min_len = min(len(benchmark_prices), len(target_prices))
        if min_len == 0:
            return None

        benchmark = benchmark_prices['close'].tail(min_len).values
        target = target_prices['close'].tail(min_len).values

        # 最新のdays個のデータを使用
        if len(benchmark) > days:
            benchmark = benchmark[-days:]
            target = target[-days:]

        # ゼロ除算を防ぐ
        if np.any(benchmark == 0):
            return None

        rs = target / benchmark
        return rs

    def calculate_rs_percentile(self, rs_values):
        """
        RS STS % (パーセンタイル)を計算
        """
        if rs_values is None or len(rs_values) == 0:
            return 0

        latest_rs = rs_values[-1]
        percentile = (np.sum(rs_values <= latest_rs) / len(rs_values)) * 100

        return round(percentile, 2)

    def collect_section_data(self, tickers_dict, benchmark_prices_short=None, skip_relative_strength=False):
        """
        セクションデータを収集

        Args:
            tickers_dict: ティッカーと表示名の辞書
            benchmark_prices_short: ベンチマークの短期価格データ
            skip_relative_strength: Relative StrengthとRS STS %をスキップするか

        Returns:
            list: ダッシュボードデータのリスト
        """
        dashboard_data = []

        for ticker, display_name in tickers_dict.items():
            print(f"{ticker} のデータを処理中...")

            # 長期の価格データ取得
            prices_long = self.get_historical_prices(ticker, days=300)
            if prices_long is None:
                print(f"  スキップ: {ticker}")
                continue

            # リアルタイム株価情報
            quote = self.get_quote(ticker)

            # 最新価格と1日の変化率
            if quote:
                latest_price = quote.get('price', prices_long['close'].iloc[-1])
                pct_change_1d = quote.get('changesPercentage', 0)
            else:
                latest_price = prices_long['close'].iloc[-1]
                prev_price = prices_long['close'].iloc[-2] if len(prices_long) > 1 else latest_price
                pct_change_1d = ((latest_price - prev_price) / prev_price) * 100

            # 相対強度計算
            if skip_relative_strength:
                rs_values = None
                rs_sparkline = ""
                rs_percentile = None
            else:
                if ticker == 'SPY':
                    rs_values = np.ones(25)
                    rs_sparkline = "━" * 20
                else:
                    rs_values = self.calculate_relative_strength(
                        benchmark_prices_short,
                        prices_long.tail(25),
                        days=25
                    )
                    rs_sparkline = self._create_sparkline_text(rs_values) if rs_values is not None else ""

                # RSパーセンタイル計算
                rs_percentile = self.calculate_rs_percentile(rs_values) if rs_values is not None else None

            # パフォーマンス計算
            performance = self.calculate_performance(
                prices_long,
                ['YTD', '1W', '1M', '1Y']
            )

            # 52週高値からの距離
            high_52w_distance = self.calculate_52w_high_distance(prices_long)

            # 移動平均の計算
            ma_data = self.calculate_moving_averages(prices_long)

            # 逆シンボル判定（NYICDXとVIXは逆）
            is_inverse = ticker in ['NYICDX', 'DX-Y.NYB', 'VIX', '^VIX']

            # データ行の作成
            row_data = {
                'ticker': ticker,
                'index': display_name,
                'Price': round(latest_price, 2),
                '% 1D': round(pct_change_1d, 2),
                'Relative Strength': rs_sparkline,
                'RS STS %': rs_percentile,
                '% YTD': round(performance['YTD'], 2),
                '% 1 W': round(performance['1W'], 2),
                '% 1 M': round(performance['1M'], 2),
                '% 1 Y': round(performance['1Y'], 2),
                '% From 52W High': round(high_52w_distance, 2),
                '10MA': ma_data['price_vs_10ma'],
                '20MA': ma_data['price_vs_20ma'],
                '50MA': ma_data['price_vs_50ma'],
                '200MA': ma_data['price_vs_200ma'],
                '20>50MA': ma_data['20_vs_50'],
                '50>200MA': ma_data['50_vs_200'],
                'is_inverse': is_inverse,
                'skip_rs': skip_relative_strength
            }

            dashboard_data.append(row_data)

            # API制限を考慮した待機
            time.sleep(0.4)

        return dashboard_data

    def create_unified_dashboard(self, sections_config, benchmark='SPY'):
        """
        統合ダッシュボードを作成（すべてのセクションを1つのシートに縦に配置）

        Args:
            sections_config: セクション設定のリスト
                [{'name': 'Market', 'tickers': {...}, 'skip_rs': False}, ...]
            benchmark: ベンチマークティッカー
        """
        # シートの作成または取得
        sheet_name = 'Dashboard'
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(
                title=sheet_name,
                rows=200,
                cols=20
            )

        # ベンチマークのデータを取得
        print(f"ベンチマーク {benchmark} のデータを取得中...")
        benchmark_prices_short = self.get_historical_prices(benchmark, days=25)

        if benchmark_prices_short is None:
            print("エラー: ベンチマークデータの取得に失敗しました")
            return

        # 各セクションのデータを収集
        all_sections_data = []
        for section in sections_config:
            print(f"\n=== {section['name']} セクション処理中 ===")
            section_data = self.collect_section_data(
                section['tickers'],
                benchmark_prices_short if not section['skip_rs'] else None,
                section['skip_rs']
            )
            all_sections_data.append({
                'name': section['name'],
                'data': section_data
            })

        # データをシートに書き込み
        self._write_unified_sheet(worksheet, all_sections_data)

        print(f"\n'{sheet_name}' シート完了!")

    def _create_sparkline_text(self, rs_values, length=20):
        """
        RS値からテキストベースのスパークラインを作成
        """
        if rs_values is None or len(rs_values) == 0:
            return ""

        # 値を0-1の範囲に正規化
        min_val = np.min(rs_values)
        max_val = np.max(rs_values)

        if max_val - min_val == 0:
            return "━" * length

        normalized = (rs_values - min_val) / (max_val - min_val)

        # バーチャート文字を使用
        bars = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█']

        # 最新のlength個のデータポイントを使用
        recent_values = normalized[-length:] if len(normalized) > length else normalized

        sparkline = ''.join([bars[min(int(v * len(bars)), len(bars)-1)] for v in recent_values])

        return sparkline

    def _write_unified_sheet(self, worksheet, sections_data):
        """
        統合シートにデータを書き込み
        """
        current_row = 1

        for section in sections_data:
            section_name = section['name']
            data = section['data']

            # セクションヘッダー行
            section_header = [[section_name]]
            worksheet.update(f'A{current_row}', section_header)

            # セクションヘッダーの書式設定
            section_header_format = {
                'backgroundColor': {'red': 0.1, 'green': 0.1, 'blue': 0.1},
                'textFormat': {
                    'bold': True,
                    'foregroundColor': {'red': 1, 'green': 1, 'blue': 1},
                    'fontSize': 12
                },
                'horizontalAlignment': 'LEFT',
                'verticalAlignment': 'MIDDLE'
            }
            worksheet.format(f'A{current_row}:Q{current_row}', section_header_format)
            worksheet.merge_cells(f'A{current_row}:Q{current_row}')
            current_row += 1

            # カラムヘッダー
            headers = [
                'ticker', 'index', 'Price', '% 1D', 'Relative Strength', 'RS STS %',
                '% YTD', '% 1 W', '% 1 M', '% 1 Y',
                '% From 52W High',
                '10MA', '20MA', '50MA', '200MA', '20>50MA', '50>200MA'
            ]
            worksheet.update(f'A{current_row}', [headers])

            # カラムヘッダーの書式設定
            header_format = {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {
                    'bold': True,
                    'foregroundColor': {'red': 1, 'green': 1, 'blue': 1},
                    'fontSize': 10
                },
                'horizontalAlignment': 'CENTER',
                'verticalAlignment': 'MIDDLE'
            }
            worksheet.format(f'A{current_row}:Q{current_row}', header_format)
            current_row += 1

            # データ行を準備
            rows = []
            for item in data:
                is_inverse = item.get('is_inverse', False)
                skip_rs = item.get('skip_rs', False)

                row = [
                    item['ticker'],
                    item['index'],
                    item['Price'],
                    item['% 1D'],
                    item['Relative Strength'] if not skip_rs else '',
                    item['RS STS %'] if not skip_rs else '',
                    item['% YTD'],
                    item['% 1 W'],
                    item['% 1 M'],
                    item['% 1 Y'],
                    item['% From 52W High'],
                    self._trend_indicator(item['10MA'], is_inverse),
                    self._trend_indicator(item['20MA'], is_inverse),
                    self._trend_indicator(item['50MA'], is_inverse),
                    self._trend_indicator(item['200MA'], is_inverse),
                    self._trend_indicator(item['20>50MA'], is_inverse),
                    self._trend_indicator(item['50>200MA'], is_inverse)
                ]
                rows.append(row)

            # データを一括書き込み
            if rows:
                data_start_row = current_row
                worksheet.update(f'A{current_row}:Q{current_row + len(rows) - 1}', rows)

                # 条件付き書式を適用
                self._apply_conditional_formatting_range(
                    worksheet, data_start_row, len(rows), data
                )

                current_row += len(rows)

            # セクション間の空行
            current_row += 1

    def _trend_indicator(self, is_above, is_inverse=False):
        """
        トレンドインジケーターを返す

        Args:
            is_above: 上にあるかどうか
            is_inverse: 逆シンボルかどうか（NYICDXとVIX）

        Returns:
            str: ▲ または ▼
        """
        if is_above is None:
            return ''

        if is_inverse:
            # 逆シンボル: 上なら赤▲、下なら緑▼
            return '▲' if is_above else '▼'
        else:
            # 通常: 上なら緑▲、下なら赤▼
            return '▲' if is_above else '▼'

    def _apply_conditional_formatting_range(self, worksheet, start_row, num_rows, data):
        """
        指定範囲に条件付き書式を適用
        """
        for i in range(num_rows):
            row_num = start_row + i
            if i >= len(data):
                continue

            item = data[i]
            is_inverse = item.get('is_inverse', False)
            skip_rs = item.get('skip_rs', False)

            # % 1D列 (D列)
            self._format_performance_cell(worksheet, f'D{row_num}', item['% 1D'])

            # RS STS %列 (F列)
            if not skip_rs and item['RS STS %'] is not None:
                self._format_percentile_cell(worksheet, f'F{row_num}', item['RS STS %'])

            # Performance列 (G-K列)
            self._format_performance_cell(worksheet, f'G{row_num}', item['% YTD'])
            self._format_performance_cell(worksheet, f'H{row_num}', item['% 1 W'])
            self._format_performance_cell(worksheet, f'I{row_num}', item['% 1 M'])
            self._format_performance_cell(worksheet, f'J{row_num}', item['% 1 Y'])
            self._format_performance_cell(worksheet, f'K{row_num}', item['% From 52W High'])

            # Trend Indicators (L-Q列)
            self._format_trend_cell(worksheet, f'L{row_num}', item['10MA'], is_inverse)
            self._format_trend_cell(worksheet, f'M{row_num}', item['20MA'], is_inverse)
            self._format_trend_cell(worksheet, f'N{row_num}', item['50MA'], is_inverse)
            self._format_trend_cell(worksheet, f'O{row_num}', item['200MA'], is_inverse)
            self._format_trend_cell(worksheet, f'P{row_num}', item['20>50MA'], is_inverse)
            self._format_trend_cell(worksheet, f'Q{row_num}', item['50>200MA'], is_inverse)

    def _format_performance_cell(self, worksheet, cell, value):
        """
        パフォーマンスセルの書式設定（緑/赤）
        """
        try:
            if value > 0:
                # プラス: 緑系
                color = {'red': 0.7, 'green': 0.95, 'blue': 0.7}
            elif value < 0:
                # マイナス: 赤系
                color = {'red': 1.0, 'green': 0.7, 'blue': 0.7}
            else:
                return

            worksheet.format(cell, {
                'backgroundColor': color,
                'horizontalAlignment': 'CENTER',
                'verticalAlignment': 'MIDDLE'
            })
        except Exception:
            pass

    def _format_percentile_cell(self, worksheet, cell, value):
        """
        パーセンタイルセルの書式設定（グラデーション）
        """
        try:
            # 0-100のパーセンタイルに基づいてカラー設定
            if value >= 90:
                color = {'red': 0.0, 'green': 0.8, 'blue': 0.4}
            elif value >= 70:
                color = {'red': 0.5, 'green': 0.9, 'blue': 0.5}
            elif value >= 50:
                color = {'red': 0.9, 'green': 0.9, 'blue': 0.7}
            elif value >= 30:
                color = {'red': 1.0, 'green': 0.85, 'blue': 0.6}
            else:
                color = {'red': 1.0, 'green': 0.7, 'blue': 0.7}

            worksheet.format(cell, {
                'backgroundColor': color,
                'horizontalAlignment': 'CENTER',
                'verticalAlignment': 'MIDDLE'
            })
        except Exception:
            pass

    def _format_trend_cell(self, worksheet, cell, is_above, is_inverse):
        """
        トレンドインジケーターセルの書式設定
        """
        if is_above is None:
            return

        try:
            if is_inverse:
                # 逆シンボル: 上なら赤、下なら緑
                if is_above:
                    color = {'red': 1.0, 'green': 0.7, 'blue': 0.7}  # 赤
                else:
                    color = {'red': 0.7, 'green': 0.95, 'blue': 0.7}  # 緑
            else:
                # 通常: 上なら緑、下なら赤
                if is_above:
                    color = {'red': 0.7, 'green': 0.95, 'blue': 0.7}  # 緑
                else:
                    color = {'red': 1.0, 'green': 0.7, 'blue': 0.7}  # 赤

            worksheet.format(cell, {
                'backgroundColor': color,
                'horizontalAlignment': 'CENTER',
                'verticalAlignment': 'MIDDLE',
                'textFormat': {'fontSize': 12}
            })
        except Exception:
            pass


def main():
    """
    メイン実行関数
    """
    # 環境変数から設定を取得
    FMP_API_KEY = os.getenv('FMP_API_KEY')
    CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE', 'credentials.json')
    SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME', 'Market Dashboard')

    # API KEYのチェック
    if not FMP_API_KEY or FMP_API_KEY == 'your_api_key_here' or FMP_API_KEY == 'your_fmp_api_key_here':
        print("エラー: FMP_API_KEYが設定されていません")
        print("1. .env.exampleを.envにコピーしてください: cp .env.example .env")
        print("2. .envファイルを編集して、FMP_API_KEYを設定してください")
        print("3. APIキーは https://site.financialmodelingprep.com/developer/docs から取得できます")
        return

    # セクション設定
    sections_config = [
        {
            'name': 'Market',
            'tickers': {
                'SPY': 'S&P 500',
                'QQQ': 'NASDAQ 100',
                'MAGS': 'Magnificent Seven',
                'RSP': 'Eql Wght S&P 500',
                'QQEW': 'Eql Wght NASDAQ 100',
                'IWM': 'Russell 2000'
            },
            'skip_rs': False
        },
        {
            'name': 'Sectors',
            'tickers': {
                'EPOL': 'Poland',
                'EWG': 'Germany',
                'GLD': 'Gold',
                'KWEB': 'China',
                'IEW': 'Europe',
                'ITA': 'Aerospace / Defense',
                'CIBR': 'Cybersecurity',
                'IBIT': 'Bitcoin',
                'BLOK': 'Blockchain',
                'IAI': 'Broker',
                'NLR': 'Uranium / Nuclear',
                'XLF': 'Finance',
                'XLU': 'Utilities',
                'TAN': 'Solar',
                'UFO': 'Space',
                'XLP': 'Consumer Staples',
                'FFTY': 'IBD 50',
                'INDA': 'India',
                'ARKW': 'ARKW',
                'XLK': 'Technology',
                'XLE': 'Energy',
                'IPO': 'IPO',
                'SOXX': 'Semiconductor',
                'MDY': 'MidCap 400',
                'SCHD': 'Dividend',
                'DIA': 'Dow Jones',
                'ITB': 'Home Construction',
                'USO': 'Oil',
                'IBB': 'Biotechnology'
            },
            'skip_rs': False
        },
        {
            'name': 'Macro',
            'tickers': {
                'DX-Y.NYB': 'U.S. Dollar',
                '^VIX': 'VIX',
                'TLT': 'Bond 20+ Year'
            },
            'skip_rs': True
        }
    ]

    try:
        # ダッシュボードの作成
        dashboard = MarketDashboard(FMP_API_KEY, CREDENTIALS_FILE, SPREADSHEET_NAME)

        print("\n" + "="*80)
        print("統合マーケットダッシュボード作成開始")
        print("="*80)

        # 統合ダッシュボードの作成
        dashboard.create_unified_dashboard(
            sections_config=sections_config,
            benchmark='SPY'
        )

        print("\n" + "="*80)
        print("ダッシュボード作成完了!")
        print(f"スプレッドシートURL: {dashboard.spreadsheet.url}")
        print("="*80)

    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
