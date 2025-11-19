"""
IBD Ratings Calculator

データベースに保存された生の値（RS値、EPS要素）をパーセンタイルランキングに変換し、
最終的なレーティング（RS Rating, EPS Rating, Composite Rating）を計算します。
"""

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from ibd_database import IBDDatabase


class IBDRatingsCalculator:
    """IBDレーティング計算クラス"""

    def __init__(self, db_path: str = 'ibd_data.db'):
        """
        Args:
            db_path: データベースファイルのパス
        """
        self.db = IBDDatabase(db_path)

    def close(self):
        """リソースをクリーンアップ"""
        self.db.close()

    # ==================== パーセンタイルランキング計算 ====================

    def calculate_percentile_ranking(self, values_dict: Dict[str, float]) -> Dict[str, float]:
        """
        値の辞書をパーセンタイルランキング（0-100）に変換

        Args:
            values_dict: {ticker: value} の辞書

        Returns:
            dict: {ticker: percentile_rating} の辞書（0-100）
        """
        # Noneや無効な値を除外
        valid_values = {k: v for k, v in values_dict.items() if v is not None and not np.isnan(v)}

        if not valid_values:
            return {}

        # 値でソート
        sorted_tickers = sorted(valid_values.items(), key=lambda x: x[1])

        # パーセンタイルを計算
        percentile_dict = {}
        total = len(sorted_tickers)

        for idx, (ticker, val) in enumerate(sorted_tickers):
            # パーセンタイル: (順位 / 総数) * 100
            percentile = ((idx + 1) / total) * 100
            percentile_dict[ticker] = round(percentile, 2)

        return percentile_dict

    # ==================== RS Rating計算 ====================

    def calculate_rs_ratings(self) -> Dict[str, float]:
        """
        全銘柄のRS Ratingを計算（パーセンタイルランキング方式）

        1. データベースから全銘柄のRS値を取得
        2. RS値をパーセンタイルランキング（0-100）に変換
        3. データベースに保存

        Returns:
            dict: {ticker: rs_rating} の辞書
        """
        print("\n全銘柄のRS Ratingを計算中...")

        # 1. 全銘柄のRS値を取得
        rs_values_dict = self.db.get_all_rs_values()
        print(f"  {len(rs_values_dict)} 銘柄のRS値を取得")

        # 2. パーセンタイルランキングに変換
        rs_ratings_dict = self.calculate_percentile_ranking(rs_values_dict)
        print(f"  {len(rs_ratings_dict)} 銘柄のRS Ratingを計算")

        # 3. データベースに保存（後で一括更新）
        return rs_ratings_dict

    # ==================== EPS Rating計算 ====================

    def calculate_eps_ratings(self) -> Dict[str, float]:
        """
        全銘柄のEPS Ratingを計算（パーセンタイルランキング方式）

        処理手順:
        1. データベースから全銘柄のEPS要素を取得
        2. 各要素を個別にパーセンタイルランキング（0-100）に変換
           - 最新四半期EPS成長率
           - 前四半期EPS成長率
           - 年間EPS成長率（CAGR）
           - 収益安定性スコア
        3. 重み付けして最終EPSランキングを計算
           - 最新四半期: 50%
           - 前四半期: 20%
           - 年間成長率: 20%
           - 安定性: 10%

        Returns:
            dict: {ticker: eps_rating} の辞書
        """
        print("\n全銘柄のEPS Ratingを計算中（パーセンタイルランキング方式）...")

        # 1. 全銘柄のEPS要素を取得
        eps_components_dict = self.db.get_all_eps_components()
        print(f"  {len(eps_components_dict)} 銘柄のEPS要素を取得")

        # 2. 各要素を個別に抽出
        eps_growth_last_qtr_dict = {}
        eps_growth_prev_qtr_dict = {}
        annual_growth_rate_dict = {}
        stability_score_dict = {}

        for ticker, components in eps_components_dict.items():
            if components['eps_growth_last_qtr'] is not None:
                eps_growth_last_qtr_dict[ticker] = components['eps_growth_last_qtr']
            if components['eps_growth_prev_qtr'] is not None:
                eps_growth_prev_qtr_dict[ticker] = components['eps_growth_prev_qtr']
            if components['annual_growth_rate'] is not None:
                annual_growth_rate_dict[ticker] = components['annual_growth_rate']
            if components['stability_score'] is not None:
                stability_score_dict[ticker] = components['stability_score']

        print(f"    最新四半期成長率: {len(eps_growth_last_qtr_dict)} 銘柄")
        print(f"    前四半期成長率: {len(eps_growth_prev_qtr_dict)} 銘柄")
        print(f"    年間成長率: {len(annual_growth_rate_dict)} 銘柄")
        print(f"    安定性スコア: {len(stability_score_dict)} 銘柄")

        # 3. 各要素を個別にパーセンタイルランキングに変換
        print("  各要素をパーセンタイルランキングに変換中...")

        percentile_last_qtr = self.calculate_percentile_ranking(eps_growth_last_qtr_dict)
        percentile_prev_qtr = self.calculate_percentile_ranking(eps_growth_prev_qtr_dict)
        percentile_annual = self.calculate_percentile_ranking(annual_growth_rate_dict)
        percentile_stability = self.calculate_percentile_ranking(stability_score_dict)

        # 4. 重み付けして最終EPSランキングを計算
        print("  重み付けして最終EPSランキングを計算中...")

        # IBDの重み付け:
        # - 最新四半期: 50%
        # - 前四半期: 20%
        # - 年間成長率: 20%
        # - 安定性: 10%
        weights = {
            'last_qtr': 0.50,
            'prev_qtr': 0.20,
            'annual': 0.20,
            'stability': 0.10
        }

        eps_ratings_dict = {}

        # 全ティッカーのユニオンを取得
        all_tickers = set()
        all_tickers.update(percentile_last_qtr.keys())
        all_tickers.update(percentile_prev_qtr.keys())
        all_tickers.update(percentile_annual.keys())
        all_tickers.update(percentile_stability.keys())

        for ticker in all_tickers:
            total_score = 0
            total_weight = 0

            # 最新四半期（50%）
            if ticker in percentile_last_qtr:
                total_score += percentile_last_qtr[ticker] * weights['last_qtr']
                total_weight += weights['last_qtr']

            # 前四半期（20%）
            if ticker in percentile_prev_qtr:
                total_score += percentile_prev_qtr[ticker] * weights['prev_qtr']
                total_weight += weights['prev_qtr']

            # 年間成長率（20%）
            if ticker in percentile_annual:
                total_score += percentile_annual[ticker] * weights['annual']
                total_weight += weights['annual']

            # 安定性（10%）
            if ticker in percentile_stability:
                total_score += percentile_stability[ticker] * weights['stability']
                total_weight += weights['stability']

            # 重み付けを正規化
            if total_weight > 0:
                eps_rating = total_score / total_weight
                eps_ratings_dict[ticker] = round(eps_rating, 2)

        print(f"  {len(eps_ratings_dict)} 銘柄のEPS Ratingを計算")

        return eps_ratings_dict

    # ==================== SMR Rating計算 ====================

    def calculate_smr_components(self, ticker: str) -> Optional[Dict]:
        """
        SMR Ratingの構成要素を計算

        IBD SMR Ratingの構成要素:
        1. Sales growth (過去3四半期の売上成長率、前年同期比)
        2. Pre-tax profit margins (税引前利益率、年次)
        3. After-tax profit margins (税引後利益率、四半期)
        4. ROE (Return on Equity、年次)

        Args:
            ticker: ティッカーシンボル

        Returns:
            dict: SMR構成要素の辞書、またはNone
        """
        try:
            # 四半期損益計算書を取得（最低8四半期必要）
            income_quarterly = self.db.get_income_statements_quarterly(ticker, limit=8)
            if not income_quarterly or len(income_quarterly) < 8:
                return None

            # 年次損益計算書を取得
            income_annual = self.db.get_income_statements_annual(ticker, limit=2)
            if not income_annual or len(income_annual) < 1:
                return None

            result = {}

            # 1. Sales growth (過去3四半期の前年同期比成長率)
            sales_growth_rates = []
            for i in range(3):
                current_revenue = income_quarterly[i].get('revenue', 0)
                yoy_revenue = income_quarterly[i + 4].get('revenue', 0) if len(income_quarterly) > i + 4 else 0

                if yoy_revenue and yoy_revenue > 0 and current_revenue:
                    growth_rate = ((current_revenue - yoy_revenue) / yoy_revenue) * 100
                    sales_growth_rates.append(growth_rate)
                    result[f'sales_growth_q{i+1}'] = growth_rate
                else:
                    result[f'sales_growth_q{i+1}'] = None

            # 平均売上成長率（過去3四半期）
            if sales_growth_rates:
                result['avg_sales_growth_3q'] = np.mean(sales_growth_rates)
            else:
                result['avg_sales_growth_3q'] = None

            # 2. Pre-tax profit margins (年次)
            # FMP APIでは incomeBeforeTax を使用
            # 注: データベースに保存されていない場合があるため、計算できない可能性がある
            # この実装では、netIncomeとtaxから逆算するか、margin計算を省略
            latest_annual = income_annual[0]
            revenue_annual = latest_annual.get('revenue', 0)
            net_income_annual = latest_annual.get('net_income', 0)

            # Pre-tax marginの近似値として、net margin * 1.25 を使用（税率20%想定）
            if revenue_annual and revenue_annual > 0 and net_income_annual:
                aftertax_margin_annual = (net_income_annual / revenue_annual) * 100
                # 簡易的にpre-tax marginを推定（税率を20-25%と仮定）
                pretax_margin_annual = aftertax_margin_annual * 1.25
                result['pretax_margin_annual'] = pretax_margin_annual
            else:
                result['pretax_margin_annual'] = None

            # 3. After-tax profit margins (四半期)
            latest_quarterly = income_quarterly[0]
            revenue_quarterly = latest_quarterly.get('revenue', 0)
            net_income_quarterly = latest_quarterly.get('net_income', 0)

            if revenue_quarterly and revenue_quarterly > 0 and net_income_quarterly:
                aftertax_margin_quarterly = (net_income_quarterly / revenue_quarterly) * 100
                result['aftertax_margin_quarterly'] = aftertax_margin_quarterly
            else:
                result['aftertax_margin_quarterly'] = None

            # 4. ROE (Return on Equity、年次)
            # ROE = Net Income / Stockholders' Equity
            balance_sheet = self.db.get_balance_sheet_annual(ticker, limit=1)

            if balance_sheet and len(balance_sheet) > 0:
                stockholders_equity = balance_sheet[0].get('total_stockholders_equity')
                # total_stockholders_equityがNoneの場合、total_equityを代替として使用
                if stockholders_equity is None:
                    stockholders_equity = balance_sheet[0].get('total_equity')

                if stockholders_equity and stockholders_equity > 0 and net_income_annual:
                    # ROE = (Net Income / Stockholders' Equity) * 100
                    roe_annual = (net_income_annual / stockholders_equity) * 100
                    result['roe_annual'] = roe_annual
                else:
                    result['roe_annual'] = None
            else:
                result['roe_annual'] = None

            return result

        except Exception as e:
            return None

    def calculate_smr_ratings(self) -> Dict[str, str]:
        """
        全銘柄のSMR Ratingを計算（パーセンタイルランキング方式）

        1. データベースから全銘柄のSMR要素を取得
        2. 各要素を個別にパーセンタイルランキング（0-100）に変換
        3. 重み付けして最終SMRスコアを計算
        4. A (Top 20%) ～ E (Bottom 20%) のレーティングに変換

        重み付け（推定）:
        - Sales growth (3Q avg): 40%
        - Pre-tax profit margins: 20%
        - After-tax profit margins: 20%
        - ROE: 20%

        Returns:
            dict: {ticker: smr_rating} の辞書 (A-E)
        """
        print("\n全銘柄のSMR Ratingを計算中...")

        # 1. 全銘柄のSMR要素を取得（データベースから既に計算済みの場合）
        smr_components_dict = self.db.get_all_smr_components()

        if not smr_components_dict:
            print("  SMR要素が計算されていません。先にSMR要素を計算してください。")
            return {}

        print(f"  {len(smr_components_dict)} 銘柄のSMR要素を取得")

        # 2. 各要素を個別に抽出
        avg_sales_growth_dict = {}
        pretax_margin_dict = {}
        aftertax_margin_dict = {}
        roe_dict = {}

        for ticker, components in smr_components_dict.items():
            if components['avg_sales_growth_3q'] is not None:
                avg_sales_growth_dict[ticker] = components['avg_sales_growth_3q']
            if components['pretax_margin_annual'] is not None:
                pretax_margin_dict[ticker] = components['pretax_margin_annual']
            if components['aftertax_margin_quarterly'] is not None:
                aftertax_margin_dict[ticker] = components['aftertax_margin_quarterly']
            if components['roe_annual'] is not None:
                roe_dict[ticker] = components['roe_annual']

        print(f"    平均売上成長率: {len(avg_sales_growth_dict)} 銘柄")
        print(f"    税引前利益率: {len(pretax_margin_dict)} 銘柄")
        print(f"    税引後利益率: {len(aftertax_margin_dict)} 銘柄")
        print(f"    ROE: {len(roe_dict)} 銘柄")

        # 3. 各要素を個別にパーセンタイルランキングに変換
        print("  各要素をパーセンタイルランキングに変換中...")

        percentile_sales_growth = self.calculate_percentile_ranking(avg_sales_growth_dict)
        percentile_pretax_margin = self.calculate_percentile_ranking(pretax_margin_dict)
        percentile_aftertax_margin = self.calculate_percentile_ranking(aftertax_margin_dict)
        percentile_roe = self.calculate_percentile_ranking(roe_dict)

        # 4. 重み付けして最終SMRスコアを計算
        print("  重み付けして最終SMRスコアを計算中...")

        # IBDの重み付け（推定）:
        weights = {
            'sales_growth': 0.40,
            'pretax_margin': 0.20,
            'aftertax_margin': 0.20,
            'roe': 0.20
        }

        smr_scores_dict = {}

        # 全ティッカーのユニオンを取得
        all_tickers = set()
        all_tickers.update(percentile_sales_growth.keys())
        all_tickers.update(percentile_pretax_margin.keys())
        all_tickers.update(percentile_aftertax_margin.keys())
        all_tickers.update(percentile_roe.keys())

        for ticker in all_tickers:
            total_score = 0
            total_weight = 0

            # Sales growth (40%)
            if ticker in percentile_sales_growth:
                total_score += percentile_sales_growth[ticker] * weights['sales_growth']
                total_weight += weights['sales_growth']

            # Pre-tax margin (20%)
            if ticker in percentile_pretax_margin:
                total_score += percentile_pretax_margin[ticker] * weights['pretax_margin']
                total_weight += weights['pretax_margin']

            # After-tax margin (20%)
            if ticker in percentile_aftertax_margin:
                total_score += percentile_aftertax_margin[ticker] * weights['aftertax_margin']
                total_weight += weights['aftertax_margin']

            # ROE (20%)
            if ticker in percentile_roe:
                total_score += percentile_roe[ticker] * weights['roe']
                total_weight += weights['roe']

            # 重み付けを正規化
            if total_weight > 0:
                smr_score = total_score / total_weight
                smr_scores_dict[ticker] = round(smr_score, 2)

        # 5. スコアをA-Eのレーティングに変換
        smr_ratings_dict = {}
        for ticker, score in smr_scores_dict.items():
            if score >= 80:
                smr_ratings_dict[ticker] = 'A'  # Top 20%
            elif score >= 60:
                smr_ratings_dict[ticker] = 'B'  # Next 20%
            elif score >= 40:
                smr_ratings_dict[ticker] = 'C'  # Middle 20%
            elif score >= 20:
                smr_ratings_dict[ticker] = 'D'  # Next 20%
            else:
                smr_ratings_dict[ticker] = 'E'  # Bottom 20%

        print(f"  {len(smr_ratings_dict)} 銘柄のSMR Ratingを計算")

        return smr_ratings_dict

    # ==================== A/D Rating計算 ====================

    def calculate_ad_rating(self, ticker: str) -> Optional[str]:
        """
        A/D Rating（Accumulation/Distribution）を計算

        Args:
            ticker: ティッカーシンボル

        Returns:
            str: A, B, C, D, E のレーティング
        """
        prices_df = self.db.get_price_history(ticker, days=70)
        if prices_df is None or len(prices_df) < 65:
            return None

        try:
            # 過去13週（65営業日）のデータ
            recent_data = prices_df.tail(65)

            close = recent_data['close'].values
            high = recent_data['high'].values
            low = recent_data['low'].values
            volume = recent_data['volume'].values

            # Money Flow Multiplier (MFM) を計算
            high_low_diff = high - low
            high_low_diff = np.where(high_low_diff == 0, 0.0001, high_low_diff)

            money_flow_multiplier = ((close - low) - (high - close)) / high_low_diff

            # Money Flow Volume = MFM * Volume
            money_flow_volume = money_flow_multiplier * volume

            # 価格変化方向とボリュームの重み付き分析
            price_changes = np.diff(close) / close[:-1]

            # 上昇日と下落日のMoney Flow Volumeを集計
            up_days = price_changes > 0
            down_days = price_changes < 0

            # 最新のMFVトレンド（直近の重み付けを高める）
            weights = np.linspace(0.5, 1.5, len(money_flow_volume[1:]))

            if np.sum(up_days) > 0 and np.sum(down_days) > 0:
                # 上昇日の加重平均Money Flow Volume
                weighted_mfv_up = np.average(
                    money_flow_volume[1:][up_days],
                    weights=weights[up_days]
                )
                # 下落日の加重平均Money Flow Volume
                weighted_mfv_down = np.average(
                    money_flow_volume[1:][down_days],
                    weights=weights[down_days]
                )

                # 総合的なAccumulation/Distribution スコア
                total_mfv = np.sum(money_flow_volume)
                avg_mfv = np.mean(money_flow_volume)

                # 正規化されたスコア（-1 to +1）
                if avg_mfv != 0:
                    normalized_score = total_mfv / (abs(avg_mfv) * len(money_flow_volume))
                else:
                    normalized_score = 0

                # 上昇/下落日のボリューム比率も考慮
                vol_ratio = weighted_mfv_up / abs(weighted_mfv_down) if weighted_mfv_down != 0 else 1

                # 総合評価スコア（-1 to +1の範囲）
                combined_score = (normalized_score * 0.6) + ((vol_ratio - 1) * 0.4)

                # レーティング判定
                if combined_score >= 0.4:
                    return 'A'
                elif combined_score >= 0.15:
                    return 'B'
                elif combined_score >= -0.15:
                    return 'C'
                elif combined_score >= -0.4:
                    return 'D'
                else:
                    return 'E'

            return 'C'

        except Exception as e:
            return None

    # ==================== Industry Group RS計算 ====================

    def calculate_sector_rs_value(self, sector_perf_df: pd.DataFrame) -> Optional[float]:
        """
        セクターのRS値を計算（株価RS値と同じ方式）

        Args:
            sector_perf_df: セクターパフォーマンスのDataFrame（日付、change_percentageカラム）

        Returns:
            float: セクターのRS値
        """
        if sector_perf_df is None or len(sector_perf_df) < 252:
            return None

        try:
            # 累積リターンを計算（change_percentageから）
            sector_perf_df = sector_perf_df.copy()
            sector_perf_df['cumulative_return'] = (1 + sector_perf_df['change_percentage'] / 100).cumprod()

            # 各期間のROC（Rate of Change）を計算
            cum_return = sector_perf_df['cumulative_return'].values

            if len(cum_return) < 252:
                return None

            roc_63d = ((cum_return[-1] / cum_return[-63]) - 1) * 100 if cum_return[-63] != 0 else 0
            roc_126d = ((cum_return[-1] / cum_return[-126]) - 1) * 100 if cum_return[-126] != 0 else 0
            roc_189d = ((cum_return[-1] / cum_return[-189]) - 1) * 100 if cum_return[-189] != 0 else 0
            roc_252d = ((cum_return[-1] / cum_return[-252]) - 1) * 100 if cum_return[-252] != 0 else 0

            # IBD式の加重平均（最新四半期に40%の重み）
            sector_rs_value = 0.4 * roc_63d + 0.2 * roc_126d + 0.2 * roc_189d + 0.2 * roc_252d

            return sector_rs_value

        except Exception as e:
            return None

    def calculate_industry_group_rs(self, ticker: str) -> Optional[Dict]:
        """
        Industry Group RS（業界グループ相対強度）を計算（旧実装・非推奨）

        注意: このメソッドは使用されていません。
        現在は calculate_all_industry_group_rs() を使用しています。

        旧計算方法:
        セクターパフォーマンスデータを使用してセクターRS値を計算し、
        銘柄RS値との差分でIndustry Group RSを算出します。
        データ不足により失敗する可能性が高いため、廃止されました。

        Args:
            ticker: ティッカーシンボル

        Returns:
            dict: {
                'sector': セクター名,
                'industry': 業界名,
                'stock_rs_value': 銘柄のRS値,
                'sector_rs_value': セクターのRS値,
                'industry_group_rs_value': Industry Group RS値
            }
        """
        try:
            # 1. 銘柄のプロファイル（セクター情報）を取得
            profile = self.db.get_company_profile(ticker)
            if not profile or not profile.get('sector'):
                return None

            sector = profile['sector']
            industry = profile.get('industry')

            # 2. 銘柄のRS値を取得
            rs_values = self.db.get_all_rs_values()
            stock_rs_value = rs_values.get(ticker)

            if stock_rs_value is None:
                return None

            # 3. セクターパフォーマンスデータを取得
            sector_perf_df = self.db.get_sector_performance_history(sector, days=300)

            if sector_perf_df is None or len(sector_perf_df) < 252:
                return None

            # 4. セクターのRS値を計算
            sector_rs_value = self.calculate_sector_rs_value(sector_perf_df)

            if sector_rs_value is None:
                return None

            # 5. Industry Group RS = 銘柄RS - セクターRS（相対強度）
            # 正の値は銘柄がセクターをアウトパフォーム、負の値はアンダーパフォーム
            industry_group_rs_value = stock_rs_value - sector_rs_value

            return {
                'sector': sector,
                'industry': industry,
                'stock_rs_value': stock_rs_value,
                'sector_rs_value': sector_rs_value,
                'industry_group_rs_value': industry_group_rs_value
            }

        except Exception as e:
            return None

    def calculate_all_industry_group_rs(self) -> Dict[str, float]:
        """
        全銘柄のIndustry Group RSを計算

        IBD方式:
        1. 各業界（industry）に属する全銘柄のRS値の平均を計算
        2. 業界平均RS値を全業界間でパーセンタイルランキングに変換
        3. 各銘柄に所属業界のパーセンタイルランキングを割り当て

        この方式により、セクターパフォーマンスデータが不要で、
        既に計算済みのRS値のみを使用して効率的に計算できます。

        Returns:
            dict: {ticker: industry_group_rs_percentile} の辞書
        """
        print("\n全銘柄のIndustry Group RSを計算中...")

        # 1. 全銘柄のRS値を取得
        all_rs_values = self.db.get_all_rs_values()

        # 2. 業界ごとに銘柄をグループ化
        industry_tickers = {}  # {industry: [tickers]}
        industry_rs_values = {}  # {industry: [rs_values]}
        ticker_to_info = {}  # {ticker: (industry, sector)}

        all_tickers = self.db.get_all_tickers()

        for idx, ticker in enumerate(all_tickers):
            if (idx + 1) % 500 == 0:
                print(f"  進捗: {idx + 1}/{len(all_tickers)} 銘柄")

            try:
                profile = self.db.get_company_profile(ticker)
                if not profile:
                    continue

                industry = profile.get('industry')
                sector = profile.get('sector')

                # industryがない場合はsectorを代替として使用
                if not industry and sector:
                    industry = sector

                if not industry:
                    continue

                rs_value = all_rs_values.get(ticker)

                if rs_value is not None:
                    if industry not in industry_tickers:
                        industry_tickers[industry] = []
                        industry_rs_values[industry] = []

                    industry_tickers[industry].append(ticker)
                    industry_rs_values[industry].append(rs_value)
                    ticker_to_info[ticker] = (industry, sector)

            except Exception as e:
                continue

        print(f"  {len(industry_tickers)} 業界グループを検出")

        # 3. 各業界の平均RS値を計算（過去6ヶ月のパフォーマンスの近似値）
        industry_avg_rs = {}
        for industry, rs_list in industry_rs_values.items():
            if rs_list and len(rs_list) > 0:
                # 業界内の銘柄が3銘柄以上ある場合のみ有効な業界グループとみなす
                if len(rs_list) >= 3:
                    industry_avg_rs[industry] = np.mean(rs_list)

        print(f"  {len(industry_avg_rs)} 業界グループ（3銘柄以上）のRS平均値を計算")

        # 4. 業界平均RS値をパーセンタイルランキングに変換
        print("  業界グループRSをパーセンタイルランキングに変換中...")
        industry_rs_percentile = self.calculate_percentile_ranking(industry_avg_rs)

        # 5. データベースに保存
        success_count = 0
        for ticker, (industry, sector) in ticker_to_info.items():
            try:
                stock_rs_value = all_rs_values.get(ticker)
                industry_rs_value = industry_avg_rs.get(industry)
                industry_group_rs_percentile = industry_rs_percentile.get(industry)

                if industry_group_rs_percentile is not None:
                    self.db.insert_industry_group_rs(
                        ticker,
                        sector if sector else industry,
                        industry,
                        stock_rs_value,
                        industry_rs_value,  # 業界平均RS値
                        industry_group_rs_percentile  # 業界のパーセンタイルランキング
                    )
                    success_count += 1
            except Exception as e:
                continue

        print(f"  {success_count} 銘柄のIndustry Group RSを計算しました")

        # 6. 各銘柄に業界のパーセンタイルランキングを割り当て
        industry_group_rs_dict = {}
        for ticker, (industry, _) in ticker_to_info.items():
            percentile = industry_rs_percentile.get(industry)
            if percentile is not None:
                industry_group_rs_dict[ticker] = percentile

        print(f"  {len(industry_group_rs_dict)} 銘柄のIndustry Group RS Ratingを計算")

        return industry_group_rs_dict

    # ==================== 52週高値からの距離計算 ====================

    def calculate_52w_high_distance(self, ticker: str) -> Optional[float]:
        """52週高値からの距離を計算"""
        prices_df = self.db.get_price_history(ticker, days=252)
        if prices_df is None or len(prices_df) < 252:
            return None

        try:
            recent_data = prices_df.tail(252)
            high_52w = recent_data['high'].max()
            current_price = prices_df['close'].iloc[-1]

            distance = ((current_price - high_52w) / high_52w) * 100
            return round(distance, 2)
        except Exception as e:
            return None

    # ==================== Composite Rating計算 ====================

    def calculate_comp_rating(self, rs_rating: float, eps_rating: float, ad_rating: str,
                             price_vs_52w_high: float, smr_rating: str = None,
                             industry_group_rs: float = None) -> Optional[float]:
        """
        Composite Rating を計算

        重み配分:
        - RS Rating: 30% (ダブルウェイト)
        - EPS Rating: 30% (ダブルウェイト)
        - A/D Rating: 15%
        - SMR Rating: 10%
        - Industry Group RS: 10% (未実装)
        - 52週高値からの距離: 5%

        Args:
            rs_rating: RS Rating (0-100)
            eps_rating: EPS Rating (0-100)
            ad_rating: A/D Rating (A-E)
            price_vs_52w_high: 52週高値からの距離 (%)
            smr_rating: SMR Rating (A-E、オプション)
            industry_group_rs: Industry Group RS (0-100、オプション)

        Returns:
            float: Composite Rating (0-100)
        """
        try:
            score = 0
            weight_sum = 0

            # 1. RS Rating (30% - ダブルウェイト)
            if rs_rating is not None:
                score += rs_rating * 0.30
                weight_sum += 0.30

            # 2. EPS Rating (30% - ダブルウェイト)
            if eps_rating is not None:
                score += eps_rating * 0.30
                weight_sum += 0.30

            # 3. A/D Rating (15%)
            ad_score_map = {
                'A': 100,
                'B': 75,
                'C': 50,
                'D': 25,
                'E': 0
            }
            if ad_rating and ad_rating in ad_score_map:
                score += ad_score_map[ad_rating] * 0.15
                weight_sum += 0.15

            # 4. SMR Rating (10%)
            smr_score_map = {
                'A': 100,  # Top 20%
                'B': 75,   # Next 20%
                'C': 50,   # Middle 20%
                'D': 25,   # Next 20%
                'E': 0     # Bottom 20%
            }
            if smr_rating and smr_rating in smr_score_map:
                score += smr_score_map[smr_rating] * 0.10
                weight_sum += 0.10

            # 5. Industry Group RS (10% - オプション)
            if industry_group_rs is not None:
                score += industry_group_rs * 0.10
                weight_sum += 0.10

            # 6. 52週高値からの距離 (5%)
            if price_vs_52w_high is not None:
                if price_vs_52w_high >= -5:
                    high_score = 100
                elif price_vs_52w_high >= -15:
                    high_score = 100 - ((abs(price_vs_52w_high) - 5) * 5)
                else:
                    high_score = max(0, 50 - ((abs(price_vs_52w_high) - 15) * 3.33))

                score += high_score * 0.05
                weight_sum += 0.05

            # 重み付けを正規化
            if weight_sum > 0:
                normalized_score = score / weight_sum * 100
                return round(min(100, max(0, normalized_score)), 2)
            else:
                return None

        except Exception as e:
            return None

    # ==================== 全レーティングの計算と保存 ====================

    def calculate_all_ratings(self):
        """
        全銘柄のレーティングを計算してデータベースに保存

        1. RS Ratingを計算
        2. EPS Ratingを計算（パーセンタイルランキング方式）
        3. SMR要素を計算してデータベースに保存
        4. SMR Ratingを計算（パーセンタイルランキング方式）
        5. Industry Group RSを計算
        6. 各銘柄のA/D Rating、52W High Distance、Composite Ratingを計算
        7. データベースに保存
        """
        print(f"\n{'='*80}")
        print("全レーティング計算開始")
        print(f"{'='*80}")

        # 1. RS Ratingを計算
        rs_ratings_dict = self.calculate_rs_ratings()

        # 2. EPS Ratingを計算（パーセンタイルランキング方式）
        eps_ratings_dict = self.calculate_eps_ratings()

        # 3. SMR要素を計算してデータベースに保存
        print(f"\n全銘柄のSMR要素を計算中...")
        all_tickers = self.db.get_all_tickers()
        smr_calculated_count = 0

        for idx, ticker in enumerate(all_tickers):
            if (idx + 1) % 500 == 0:
                print(f"  進捗: {idx + 1}/{len(all_tickers)} 銘柄")

            try:
                smr_components = self.calculate_smr_components(ticker)
                if smr_components:
                    self.db.insert_calculated_smr(
                        ticker,
                        smr_components.get('sales_growth_q1'),
                        smr_components.get('sales_growth_q2'),
                        smr_components.get('sales_growth_q3'),
                        smr_components.get('avg_sales_growth_3q'),
                        smr_components.get('pretax_margin_annual'),
                        smr_components.get('aftertax_margin_quarterly'),
                        smr_components.get('roe_annual')
                    )
                    smr_calculated_count += 1
            except Exception as e:
                continue

        print(f"  {smr_calculated_count} 銘柄のSMR要素を計算・保存しました")

        # 4. SMR Ratingを計算（パーセンタイルランキング方式）
        smr_ratings_dict = self.calculate_smr_ratings()

        # 5. Industry Group RSを計算
        industry_group_rs_ratings_dict = self.calculate_all_industry_group_rs()

        # 6. 全銘柄のA/D RatingとComposite Ratingを計算
        print(f"\n全 {len(all_tickers)} 銘柄のA/D RatingとComposite Ratingを計算中...")

        # 7. 各銘柄のA/D Rating、52W High Distance、Composite Ratingを計算
        calculated_count = 0
        for idx, ticker in enumerate(all_tickers):
            if (idx + 1) % 500 == 0:
                print(f"  進捗: {idx + 1}/{len(all_tickers)} 銘柄")

            try:
                rs_rating = rs_ratings_dict.get(ticker)
                eps_rating = eps_ratings_dict.get(ticker)

                # RS RatingまたはEPS Ratingがない場合はスキップ
                if rs_rating is None and eps_rating is None:
                    continue

                # A/D Ratingを計算
                ad_rating = self.calculate_ad_rating(ticker)

                # SMR Ratingを取得
                smr_rating = smr_ratings_dict.get(ticker)

                # Industry Group RS Ratingを取得
                industry_group_rs = industry_group_rs_ratings_dict.get(ticker)

                # 52週高値からの距離を計算
                price_vs_52w_high = self.calculate_52w_high_distance(ticker)

                # Composite Ratingを計算（Industry Group RSを含む）
                comp_rating = self.calculate_comp_rating(
                    rs_rating, eps_rating, ad_rating, price_vs_52w_high, smr_rating, industry_group_rs
                )

                # データベースに保存
                self.db.insert_calculated_rating(
                    ticker,
                    rs_rating,
                    eps_rating,
                    ad_rating,
                    comp_rating,
                    price_vs_52w_high,
                    smr_rating,
                    industry_group_rs
                )

                calculated_count += 1

            except Exception as e:
                continue

        print(f"  {calculated_count} 銘柄のレーティングを計算・保存しました")

        print(f"\n{'='*80}")
        print("全レーティング計算完了!")
        print(f"{'='*80}\n")

        # 統計表示
        self.db.get_database_stats()


def main():
    """テスト実行"""
    try:
        calculator = IBDRatingsCalculator()

        # 全レーティングを計算
        calculator.calculate_all_ratings()

        calculator.close()

    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
