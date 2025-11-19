# Dev Container セットアップガイド

このプロジェクトは VS Code の Dev Container 機能に対応しています。

## 必要なもの

- [Visual Studio Code](https://code.visualstudio.com/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- VS Code 拡張機能: [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

## 使い方

### 1. Dev Container を開く

1. このプロジェクトを VS Code で開く
2. コマンドパレット (Ctrl+Shift+P / Cmd+Shift+P) を開く
3. "Dev Containers: Reopen in Container" を選択
4. コンテナのビルドと起動を待つ（初回は数分かかります）

### 2. 環境変数の設定

FMP API を使用する場合は、環境変数を設定してください：

```bash
export FMP_API_KEY='your_fmp_api_key_here'
```

または、`.env` ファイルをプロジェクトルートに作成：

```bash
FMP_API_KEY=your_fmp_api_key_here
```

### 3. 開発開始

Dev Container が起動すると、以下が自動的にインストールされます：

- Python 3.12
- プロジェクトの依存関係 (requirements.txt)
- 開発ツール (pytest, black, flake8, mypy, pylint)
- VS Code 拡張機能 (Python, Pylance, Jupyter など)

## 主な機能

### インストール済みツール

- **Python 開発環境**: Python 3.12 + pip
- **テストフレームワーク**: pytest, pytest-cov
- **コードフォーマッター**: black, autopep8
- **リンター**: flake8, pylint, mypy
- **対話型シェル**: IPython
- **Jupyter Notebook**: サポート済み

### VS Code 拡張機能

- Python
- Pylance (高度な型チェック)
- Black Formatter
- Jupyter
- Python Environment Manager
- autoDocstring

### ポートフォワーディング

以下のポートが自動的にフォワードされます：

- `8050`: ダッシュボード用
- `8888`: Jupyter Notebook 用

## 開発ワークフロー

### パッケージのインストール

```bash
# 開発モードでインストール
pip install -e .
```

### テストの実行

```bash
# すべてのテストを実行
pytest

# カバレッジレポート付き
pytest --cov=stagealgo
```

### コードフォーマット

```bash
# Black でフォーマット
black .

# フォーマットチェックのみ
black --check .
```

### リンターの実行

```bash
# Flake8
flake8 .

# Pylint
pylint stagealgo/

# MyPy (型チェック)
mypy stagealgo/
```

### スクリーナーの実行

```bash
# スクリーナーを実行
stagealgo-screener --input stagealgo/data/stock.csv --output results/

# ダッシュボードを起動
stagealgo-dashboard
```

## トラブルシューティング

### コンテナが起動しない

1. Docker Desktop が起動していることを確認
2. VS Code を再起動
3. コマンドパレット → "Dev Containers: Rebuild Container"

### パッケージが見つからない

```bash
# 依存関係を再インストール
pip install -r requirements.txt
pip install -e .
```

### 権限エラー

コンテナは root ユーザーで実行されています。権限の問題が発生した場合は、
`devcontainer.json` の `remoteUser` を変更してください。

## その他

- コンテナ内でも Git 操作が可能です
- Docker socket がマウントされているため、Docker-in-Docker も可能です
- ワークスペースは `/workspace` にマウントされます
