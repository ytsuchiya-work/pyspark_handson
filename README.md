# pyspark_handson

PySpark を Databricks 上で試すハンズオンです。合成データの生成から、DataFrame 操作・結合・集計・SQL 連携・各種データソースの読み書きまでを一気通貫で体験できます。

## 構成

| パス | 内容 |
|------|------|
| `notebooks/01_pyspark_handson.py` | ハンズオン用 Databricks ノートブック（データ生成 → PySpark 実行） |
| `docs/pyspark_handson.html` | PySpark の概要・実装・注意点・手順をまとめた HTML 資料 |

## 実行手順

1. Databricks ワークスペースでこのリポジトリを **Git フォルダ** として追加（または最新へ Pull）。
2. `notebooks/01_pyspark_handson.py` を開く。
3. Serverless もしくは Databricks Runtime 15.4 LTS 以降の汎用クラスターにアタッチ。
4. 上部ウィジェットで出力先カタログ・スキーマ、生成件数を設定。
5. 上から順にセルを実行。

## 学べる内容

- `createDataFrame` / `spark.range` による DataFrame 作成（遅延評価）
- 合成データの分散生成と Delta テーブルへの保存（`saveAsTable`）
- 列操作（`select` / `withColumn` / `cast` / `withColumnRenamed`）
- 行操作（`filter` / `distinct` / `sort` / `limit`）
- 結合（`join`）・集計（`groupBy` + `agg`）・メソッドチェーン
- SQL 連携（`createOrReplaceTempView` + `spark.sql`）
- データソースの読み書き（CSV / JSON / Parquet on Volume）

## 参考

- [Databricks の PySpark](https://docs.databricks.com/aws/ja/pyspark/)
- [PySpark の基本](https://docs.databricks.com/aws/ja/pyspark/basics)
- [PySpark のデータソース](https://docs.databricks.com/aws/ja/pyspark/datasources)
