# pyspark_handson

PySpark を Databricks 上で試すハンズオンです。合成データの生成から、DataFrame 操作・結合・集計・SQL 連携・各種データソースの読み書きまでを一気通貫で体験できます。

## 構成

| パス | 内容 |
|------|------|
| `notebooks/01_pyspark_handson.py` | ハンズオン用 Databricks ノートブック（データ生成 → PySpark 実行。初級→中級） |
| `docs/pyspark_handson.html` | PySpark の概要・実装・注意点・手順をまとめた HTML 資料 |
| `docs/pyspark_handson.pdf` | 上記 HTML の PDF 版 |

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
- SQL 連携（`createOrReplaceTempView` + `spark.sql` / `selectExpr` / `expr`）
- データ型操作（`when`/`otherwise`・`coalesce`・日付・複雑な型）
- データソースの読み書き（CSV / JSON / Parquet on Volume）
- DataFrame in/out 設計と単体テスト、スキーマに基づく動的処理
- **Delta Lake**（ACID・`DELETE`/`UPDATE`/`MERGE`・タイムトラベル・`OPTIMIZE`）
- パフォーマンスチューニング（broadcast join・`explain`）・pandas/Arrow 連携・UDF

## 参考

- [Databricks の PySpark](https://docs.databricks.com/aws/ja/pyspark/)
- [PySpark の基本](https://docs.databricks.com/aws/ja/pyspark/basics)
- [PySpark のデータソース](https://docs.databricks.com/aws/ja/pyspark/datasources)
- [The Data Engineer's Guide to Apache Spark and Delta Lake（Databricks eBook）](https://www.databricks.com/resources/ebook/the-data-engineers-guide-to-apache-spark-and-delta-lake)
- [PySpark開発時に最低限知っておくべき7つの知識（manabian, Qiita）](https://qiita.com/manabian/items/9117ac98246dd8bb6edf)
- [Pythonで大量データ処理！PySparkを用いたデータ処理と分析のきほん（Chie Hayashida, SpeakerDeck）](https://speakerdeck.com/chie8842/pythondeda-liang-detachu-li-pysparkwoyong-itadetachu-li-tofen-xi-falsekihon)
