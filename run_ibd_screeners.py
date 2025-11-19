"""
IBD Screeners 実行スクリプト

このスクリプトは以下のワークフローを実行します：
1. データ収集: 全銘柄の株価・EPS・プロファイルデータをFMP APIから取得してDBに保存
2. レーティング計算: RS Rating、EPS Rating（パーセンタイルランキング方式）、Composite Ratingを計算
3. スクリーナー実行: 各IBDスクリーナーを実行して結果をGoogleスプレッドシートに出力

使用方法:
    python run_ibd_screeners.py [--collect-data] [--calculate-ratings] [--run-screeners]

オプション:
    --collect-data: データ収集を実行
    --calculate-ratings: レーティング計算を実行
    --run-screeners: スクリーナーを実行
    引数なし: 全ステップを実行
"""

import os
import argparse
from dotenv import load_dotenv

from ibd_data_collector import IBDDataCollector
from ibd_ratings_calculator import IBDRatingsCalculator
from ibd_screeners import IBDScreeners


def main():
    # 環境変数から設定を取得
    load_dotenv()

    FMP_API_KEY = os.getenv('FMP_API_KEY')
    CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE', 'credentials.json')
    SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME', 'Market Dashboard')
    MAX_WORKERS = int(os.getenv('ORATNEK_MAX_WORKERS', '3'))  # Default: 3 (Starter), Recommended: 6 (Premium), 10+ (Professional)
    DB_PATH = os.getenv('IBD_DB_PATH', 'ibd_data.db')

    # API KEYのチェック
    if not FMP_API_KEY or FMP_API_KEY == 'your_api_key_here' or FMP_API_KEY == 'your_fmp_api_key_here':
        print("エラー: FMP_API_KEYが設定されていません")
        print("1. .env.exampleを.envにコピーしてください: cp .env.example .env")
        print("2. .envファイルを編集して、FMP_API_KEYを設定してください")
        print("3. APIキーは https://site.financialmodelingprep.com/developer/docs から取得できます")
        return

    # コマンドライン引数のパース
    parser = argparse.ArgumentParser(description='IBD Screeners 実行スクリプト')
    parser.add_argument('--collect-data', action='store_true', help='データ収集を実行')
    parser.add_argument('--calculate-ratings', action='store_true', help='レーティング計算を実行')
    parser.add_argument('--run-screeners', action='store_true', help='スクリーナーを実行')
    parser.add_argument('--test-mode', action='store_true', help='テストモード（500銘柄に制限）')
    parser.add_argument('--debug', action='store_true', help='デバッグモード（詳細なエラー表示）')
    parser.add_argument('--sample', type=int, default=None, help='サンプル銘柄数（テスト用）')
    args = parser.parse_args()

    # 引数がない場合は全ステップを実行
    run_all = not (args.collect_data or args.calculate_ratings or args.run_screeners)

    try:
        # ステップ1: データ収集
        if run_all or args.collect_data:
            print("\n" + "="*80)
            print("ステップ1: データ収集")
            print("="*80)

            collector = IBDDataCollector(FMP_API_KEY, db_path=DB_PATH, debug=args.debug)

            # サンプルモード
            if args.sample:
                print(f"サンプルモード: 最初の{args.sample}銘柄のみ処理")
                from get_tickers import FMPTickerFetcher
                fetcher = FMPTickerFetcher()
                tickers_df = fetcher.get_all_stocks(['nasdaq', 'nyse'])
                sample_tickers = tickers_df['Ticker'].tolist()[:args.sample]
                collected = collector.collect_all_data(sample_tickers, max_workers=1)
            else:
                collector.run_full_collection(
                    use_full_dataset=not args.test_mode,
                    max_workers=MAX_WORKERS
                )
            collector.close()

        # ステップ2: レーティング計算
        if run_all or args.calculate_ratings:
            print("\n" + "="*80)
            print("ステップ2: レーティング計算")
            print("="*80)

            calculator = IBDRatingsCalculator(db_path=DB_PATH)
            calculator.calculate_all_ratings()
            calculator.close()

        # ステップ3: スクリーナー実行
        if run_all or args.run_screeners:
            print("\n" + "="*80)
            print("ステップ3: スクリーナー実行")
            print("="*80)

            # データ収集をスキップした場合、ベンチマークデータを確認
            if not run_all and args.run_screeners and not args.collect_data:
                print("\nベンチマークデータを確認中...")
                from ibd_database import IBDDatabase
                db = IBDDatabase(DB_PATH, silent=True)
                spy_data = db.get_price_history('SPY', days=30)
                db.close()

                if spy_data is None or len(spy_data) < 25:
                    print("⚠ SPYデータが不足しています。取得中...")
                    collector = IBDDataCollector(FMP_API_KEY, db_path=DB_PATH)
                    collector.collect_benchmark_data(['SPY'])
                    collector.close()

            screeners = IBDScreeners(
                credentials_file=CREDENTIALS_FILE,
                spreadsheet_name=SPREADSHEET_NAME,
                db_path=DB_PATH
            )
            screeners.run_all_screeners()
            screeners.close()

        print("\n" + "="*80)
        print("全処理が完了しました！")
        print("="*80)

    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
