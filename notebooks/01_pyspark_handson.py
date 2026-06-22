# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark ハンズオン on Databricks（初級 → 中級）
# MAGIC
# MAGIC このノートブックは、Databricks 上で **PySpark** を初級から中級レベルまで体系的に学ぶハンズオンです。
# MAGIC Spark の考え方・アーキテクチャの理解から始め、DataFrame 操作、SQL 連携、実践的なデータエンジニアリング設計、
# MAGIC パフォーマンスチューニングまでを、合成データを使って手を動かしながら習得します。
# MAGIC
# MAGIC ## 構成
# MAGIC | パート | 対象 | 内容 |
# MAGIC |---|---|---|
# MAGIC | **Part 1** | 初級 | Spark の概念・アーキテクチャ / 遅延評価 / SparkSession / DataFrame 作成 / 基本操作 / データソース |
# MAGIC | **Part 2** | 中級 | 結合・集計 / SQL 連携 / メタデータ操作 / DataFrame in-out 設計 + 単体テスト / 動的処理 / 性能チューニング / pandas 連携 / UDF |
# MAGIC
# MAGIC ## 参考資料
# MAGIC - [Databricks の PySpark（概要 / 基本 / データソース）](https://docs.databricks.com/aws/ja/pyspark/)
# MAGIC - [PySpark開発時に最低限知っておくべき7つの知識（manabian, Qiita）](https://qiita.com/manabian/items/9117ac98246dd8bb6edf)
# MAGIC - Chie Hayashida「Pythonで大量データ処理！PySparkを用いたデータ処理と分析のきほん」(PyCon JP 2017)
# MAGIC
# MAGIC **実行環境**: Serverless もしくは Databricks Runtime 15.4 LTS 以降の汎用クラスター

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. パラメータ設定
# MAGIC ハンズオン用の出力先カタログ・スキーマ、生成件数をウィジェットで指定します。

# COMMAND ----------

dbutils.widgets.text("catalog", "classic_stable_ytcy_catalog", "出力先カタログ")
dbutils.widgets.text("schema", "pyspark_handson", "出力先スキーマ")
dbutils.widgets.text("num_customers", "10000", "生成する顧客数")
dbutils.widgets.text("num_orders", "50000", "生成する注文数")

catalog_name = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
num_customers = int(dbutils.widgets.get("num_customers"))
num_orders = int(dbutils.widgets.get("num_orders"))
volume_name = "handson_files"

print(f"catalog       = {catalog_name}")
print(f"schema        = {schema_name}")
print(f"num_customers = {num_customers:,}")
print(f"num_orders    = {num_orders:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.1 セットアップ（スキーマ・ボリューム作成）
# MAGIC Unity Catalog 上にハンズオン用のスキーマと、ファイル入出力用のマネージドボリュームを作成します。

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{volume_name}`")
spark.sql(f"USE `{catalog_name}`.`{schema_name}`")

volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
print(f"作成済みスキーマ : {catalog_name}.{schema_name}")
print(f"ボリュームパス   : {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1 — 初級：PySpark の基礎
# MAGIC
# MAGIC ## 1.1 Apache Spark と PySpark とは
# MAGIC
# MAGIC - **Apache Spark** は OSS の並列分散処理フレームワーク。障害リカバリ・タスク分割・スケジューリングを Spark が肩代わりし、
# MAGIC   サーバーを増やす（スケールアウト）ことでスループットがほぼ線形に向上します。
# MAGIC - **高速な理由**：オンメモリ処理 / JVM オーバーヘッドを抑える Project Tungsten / キャッシュ / **遅延評価**。
# MAGIC - **PySpark** は Python から Spark を操作する API。Python の手軽さと Spark の分散処理を両立できます。
# MAGIC - **Databricks** は Apache Spark の上に構築されたプラットフォームで、ノートブックに `spark`（SparkSession）と `dbutils` が初期化済みです。
# MAGIC
# MAGIC ### アーキテクチャの要点（中級への布石）
# MAGIC - PySpark は Python ↔ JVM を **Py4J** で橋渡しします。
# MAGIC - **DataFrame** の処理はワーカー上の **JVM** で実行されるため高速です。
# MAGIC - 一方 **RDD や Python UDF** はワーカー上の **Python プロセス**で実行され、シリアライズ（pickling）のオーバーヘッドが生じます。
# MAGIC - → だから「**できるだけ DataFrame / 組み込み関数を使う**」のが鉄則です（Part 2 で実演）。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 SparkSession（spark）
# MAGIC すべての入り口は `SparkSession`。Databricks では `spark` として用意済みですが、外部スクリプトや関数内では明示的に取得します。

# COMMAND ----------

from pyspark.sql import SparkSession

# Databricks 外（スクリプト実行）ではこう取得する
# spark = SparkSession.builder.getOrCreate()

# 共通関数の内部などでは、既存のセッションを取得して再利用する
active = SparkSession.getActiveSession()
print("Spark version :", active.version)
print("アプリ名       :", active.sparkContext.appName if active else "N/A")

# 実行時設定は spark.conf で管理（取得の例）
print("shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 遅延評価：変換（Transformation）とアクション（Action）
# MAGIC PySpark の最重要概念。**変換**を積み重ねても計算は実行されず、**アクション**で初めて実行されます。
# MAGIC `explain()` で「実行計画だけが組み立てられている」ことを確認できます。

# COMMAND ----------

# ここまでは "定義" のみ。データはまだ動いていない（遅延評価）
from pyspark.sql import functions as F
lazy_df = (
    spark.range(1, 1000)
    .withColumnRenamed("id", "n")
    .filter("n % 2 = 0")                    # 変換：偶数のみ
    .withColumn("squared", F.col("n") ** 2)  # 変換：二乗列
)
print("ここまで何も計算されていません（変換の定義のみ）")

# 実行計画を確認（アクションではないが計画を見られる）
lazy_df.explain(mode="formatted")

# アクションを呼ぶと初めて計算される
print("件数（アクション count を実行）:", lazy_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 DataFrame の作成（4 つの方法）
# MAGIC 用途に応じて複数の作り方があります。スキーマは `"col1 type, col2 type"` の DDL 文字列で簡潔に指定できます。

# COMMAND ----------

import datetime
from pyspark.sql import Row

schema = "str_col string, int_col integer, date_col date"

# 方法1: 辞書のリスト
df_dict = spark.createDataFrame(
    [{"str_col": "abc", "int_col": 123, "date_col": datetime.date(2020, 1, 1)},
     {"str_col": "def", "int_col": 456, "date_col": datetime.date(2020, 1, 2)}],
    schema,
)

# 方法2: 多次元リスト（またはタプル）
df_list = spark.createDataFrame(
    [["abc", 123, datetime.date(2020, 1, 1)],
     ["def", 456, datetime.date(2020, 1, 2)]],
    schema,
)

# 方法3: pyspark.sql.Row
df_row = spark.createDataFrame(
    [Row(str_col="abc", int_col=123, date_col=datetime.date(2020, 1, 1)),
     Row(str_col="def", int_col=456, date_col=datetime.date(2020, 1, 2))],
    schema,
)

# 方法4: 値を直接指定（列名のみ、型は推論）
df_children = spark.createDataFrame(
    data=[("Mikhail", 15), ("Zaky", 13), ("Zoya", 8)],
    schema=["name", "age"],
)

print("方法1〜3 は同じ内容になります:")
display(df_dict)
display(df_children)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 メタデータ（スキーマ）の確認
# MAGIC DataFrame は不変（イミュータブル）。まずは構造を正しく把握することが大切です。

# COMMAND ----------

print("=== printSchema ===")
df_dict.printSchema()
print("=== columns ===", df_dict.columns)
print("=== dtypes ===", df_dict.dtypes)
print("=== DDL 文字列 (simpleString) ===")
print(df_dict.schema.simpleString())
print("=== JSON 形式のフィールド情報 ===")
print(df_dict.schema.jsonValue()["fields"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 合成データの生成（顧客・注文）
# MAGIC `spark.range` と `pyspark.sql.functions` を組み合わせ、**分散処理に乗る形**で大規模データを生成します。
# MAGIC Python の `for` で 1 行ずつ作るのではなく、関数で列を一括生成するのが PySpark 流です。

# COMMAND ----------

from pyspark.sql import functions as F

segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
nations = ["JAPAN", "UNITED STATES", "GERMANY", "FRANCE", "INDIA", "BRAZIL"]

df_customer = (
    spark.range(1, num_customers + 1)
    .withColumnRenamed("id", "c_custkey")
    .withColumn("c_name", F.concat(F.lit("Customer#"), F.col("c_custkey").cast("string")))
    .withColumn("c_mktsegment", F.element_at(F.array(*[F.lit(s) for s in segments]),
                                             (F.col("c_custkey") % len(segments) + 1).cast("int")))
    .withColumn("c_nationkey", (F.col("c_custkey") % len(nations)).cast("int"))
    .withColumn("c_nation", F.element_at(F.array(*[F.lit(n) for n in nations]),
                                         (F.col("c_custkey") % len(nations) + 1).cast("int")))
    .withColumn("c_acctbal", F.round(F.rand(seed=42) * 10000 - 1000, 2))
)
display(df_customer)

# COMMAND ----------

order_status = ["O", "F", "P"]
order_priority = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]

df_order = (
    spark.range(1, num_orders + 1)
    .withColumnRenamed("id", "o_orderkey")
    .withColumn("o_custkey", (F.floor(F.rand(seed=1) * num_customers) + 1).cast("long"))
    .withColumn("o_orderstatus", F.element_at(F.array(*[F.lit(s) for s in order_status]),
                                              (F.col("o_orderkey") % len(order_status) + 1).cast("int")))
    .withColumn("o_orderpriority", F.element_at(F.array(*[F.lit(p) for p in order_priority]),
                                                (F.col("o_orderkey") % len(order_priority) + 1).cast("int")))
    .withColumn("o_totalprice", F.round(F.rand(seed=7) * 50000 + 100, 2))
    .withColumn("o_orderdate", F.expr("date_add(to_date('2023-01-01'), cast(rand(11) * 730 as int))"))
)
display(df_order)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Delta テーブルとして保存し、読み込む
# MAGIC **テーブル = メタストアの定義**、**データ = ストレージ上のファイル** という分離を意識します。
# MAGIC Databricks ではマネージド Delta テーブルとして保存するのが基本です。

# COMMAND ----------

df_customer.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.customer")
df_order.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.orders")

# 読み込みは spark.table
df_customer = spark.table(f"{catalog_name}.{schema_name}.customer")
df_order = spark.table(f"{catalog_name}.{schema_name}.orders")
print(f"customer 件数 : {df_customer.count():,}")
print(f"orders   件数 : {df_order.count():,}")
display(spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 列・行の基本操作
# MAGIC `select` / `withColumn` / `cast` / `withColumnRenamed`（列）、`filter` / `distinct` / `sort` / `limit`（行）。

# COMMAND ----------

from pyspark.sql.functions import col

# 列の選択・追加・型変換・リネーム
display(df_customer.select(col("c_custkey"), col("c_name"), col("c_acctbal")))
df_flag = df_customer.withColumn("balance_flag", col("c_acctbal") > 1000)
display(df_flag.select("c_custkey", "c_acctbal", "balance_flag"))

# 行のフィルタ（複数条件は括弧 + & / |）
display(df_customer.filter((col("c_mktsegment") == "BUILDING") & (col("c_acctbal") > 5000)))

# 重複削除・ソート・上位 N 件
display(df_customer.select("c_mktsegment").distinct())
display(df_customer.sort(col("c_acctbal").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 データソース：各種フォーマットの書き込み・読み込み
# MAGIC ボリューム上に CSV / JSON / Parquet で書き出し、読み戻します。分析用途では **Parquet（列指向・高速）** が定番です。

# COMMAND ----------

sample = df_customer.select("c_custkey", "c_name", "c_mktsegment", "c_acctbal").limit(100)
(sample.write.format("csv").option("header", True).mode("overwrite").save(f"{volume_path}/customer_csv"))
(sample.write.format("json").mode("overwrite").save(f"{volume_path}/customer_json"))
(sample.write.format("parquet").mode("overwrite").save(f"{volume_path}/customer_parquet"))

df_csv = (spark.read.format("csv").option("header", True).option("inferSchema", True)
          .load(f"{volume_path}/customer_csv"))
print("CSV 読み込み（ヘッダー + スキーマ推論）:")
display(df_csv)
print("Parquet 件数:", spark.read.format("parquet").load(f"{volume_path}/customer_parquet").count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2 — 中級：実践的なデータエンジニアリング
# MAGIC
# MAGIC ## 2.1 結合・集計・メソッドチェーン

# COMMAND ----------

from pyspark.sql.functions import avg, count, sum as _sum  # 組み込み sum と衝突するため別名

# 結合（inner / left / outer）
df_joined = df_order.join(
    df_customer,
    on=df_order["o_custkey"] == df_customer["c_custkey"],
    how="inner",
)
display(df_joined.select("o_orderkey", "o_totalprice", "c_name", "c_mktsegment", "c_nation").limit(20))

# 集計（groupBy + agg）
display(
    df_customer.groupBy("c_mktsegment").agg(
        avg("c_acctbal").alias("avg_balance"),
        count("*").alias("n_customers"),
    ).sort(col("avg_balance").desc())
)

# メソッドチェーンで読みやすく
df_chained = (
    df_order.filter(col("o_orderstatus") == "F")
    .groupBy(col("o_orderpriority"))
    .agg(count(col("o_orderkey")).alias("n_orders"),
         _sum(col("o_totalprice")).alias("total_price"))
    .sort(col("n_orders").desc())
)
display(df_chained)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Spark SQL を PySpark から使う（4 つのスタイル）
# MAGIC PySpark と SQL は相互運用できます。チームの得意な書き方を選べます。

# COMMAND ----------

# 一時ビューに登録
df_joined.createOrReplaceTempView("joined_view")

# スタイル1: spark.sql（純粋な SQL）
df_sql = spark.sql("""
    SELECT c_nation, c_mktsegment,
           COUNT(*) AS n_orders,
           ROUND(SUM(o_totalprice), 2) AS revenue
    FROM joined_view
    GROUP BY c_nation, c_mktsegment
    ORDER BY revenue DESC
    LIMIT 15
""")
display(df_sql)

# スタイル2: selectExpr（SQL 式で列を選択・変換）
display(df_customer.selectExpr("c_custkey", "upper(c_mktsegment) AS segment_upper",
                               "CAST(c_acctbal AS int) AS balance_int").limit(5))

# スタイル3: expr（withColumn の中で SQL 式）
from pyspark.sql.functions import expr
display(df_customer.withColumn("balance_band",
        expr("CASE WHEN c_acctbal > 5000 THEN 'high' WHEN c_acctbal > 0 THEN 'mid' ELSE 'neg' END")
        ).select("c_custkey", "c_acctbal", "balance_band").limit(5))

# スタイル4: filter / where に SQL 文字列、alias で別名
display(df_customer.alias("c").filter("c.c_acctbal > 9000").select("c.c_custkey", "c.c_acctbal").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 メタデータと値の取得（collect の作法）
# MAGIC `collect()` は**全データをドライバーに集める**ため、大量データでは使わず、必ず事前に絞り込みます。

# COMMAND ----------

# 単一値の取得：いったん 1 行 1 列に絞ってから collect
one = df_customer.filter("c_custkey = 1").select("c_name")
row = one.collect()[0]
print("位置指定 :", row[0])
print("キー指定 :", row["c_name"])
print("属性指定 :", row.c_name)

# 複数値をリスト化（小さく絞ってから）
top_names = [r["c_name"] for r in df_customer.sort(col("c_acctbal").desc()).limit(5).collect()]
print("残高トップ5の顧客名:", top_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 「DataFrame in, DataFrame out」設計と単体テスト
# MAGIC **入力も出力も DataFrame** にする関数設計は、再利用性とテスト容易性を高める王道パターンです（Qiita 記事より）。

# COMMAND ----------

def cast_int_col_as_int(df):
    """int_col を integer 型にキャストして返す（DataFrame in / DataFrame out）。"""
    return df.withColumn("int_col", df.int_col.cast("int"))


# --- 単体テスト（ノートブック内で assert 実行）---
def test_cast_int_col_as_int():
    test_df = spark.createDataFrame([{"int_col": "1"}, {"int_col": "12.3"}], "int_col string")
    expected = spark.createDataFrame([{"int_col": 1}, {"int_col": 12}], "int_col integer")
    result = cast_int_col_as_int(test_df)
    assert result.collect() == expected.collect(), "変換結果が期待値と一致しません"
    print("✅ test_cast_int_col_as_int: PASS")


test_cast_int_col_as_int()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 変数・クラス・Config の活用
# MAGIC 不変な DataFrame を辞書に貯めて処理過程を追跡したり、`staticmethod` で関数を整理したりできます。

# COMMAND ----------

# 処理過程を辞書に保持してデバッグしやすくする
stages = {}
stages["raw"] = spark.createDataFrame([{"int_col": "1"}, {"int_col": "2"}], "int_col string")
stages["casted"] = cast_int_col_as_int(stages["raw"])
print("raw    dtypes:", stages["raw"].dtypes)
print("casted dtypes:", stages["casted"].dtypes)


# 共通処理を staticmethod でまとめ、クラス変数を Config として使う
class SparkUtilities:
    is_databricks = True  # 環境フラグ（Config）

    @staticmethod
    def show(df, n=5):
        # Databricks では display、それ以外では show を使う、といった切り替え
        if SparkUtilities.is_databricks:
            display(df.limit(n))
        else:
            df.show(n)


SparkUtilities.show(stages["casted"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 制御フローによる動的処理
# MAGIC メタデータ（スキーマ）を使って、列を動的に処理する実践テクニック。
# MAGIC ここでは「ある DataFrame のスキーマに合わせて、もう一方の列型を自動で揃える」処理と「動的な SELECT 文生成」を行います。

# COMMAND ----------

# (a) スキーマに基づく型の自動変換
df_str = spark.createDataFrame(
    [{"id": "1", "amount": "100.5", "ts": "2023-01-01"}], "id string, amount string, ts string")
target_types = {"id": "int", "amount": "double", "ts": "date"}

tgt = df_str
for c in df_str.columns:
    want = target_types.get(c)
    if want == "date":
        tgt = tgt.withColumn(c, F.to_date(F.col(c)))
    elif want:
        tgt = tgt.withColumn(c, F.col(c).cast(want))
print("自動変換後のスキーマ:")
tgt.printSchema()

# (b) 列名から SELECT 文を動的生成
cols = df_customer.columns
select_clause = "\n  , ".join(cols)
dynamic_sql = f"SELECT\n  {select_clause}\nFROM {catalog_name}.{schema_name}.customer\nLIMIT 3"
print(dynamic_sql)
display(spark.sql(dynamic_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 パフォーマンスチューニングの基本
# MAGIC - **DataFrame / 組み込み関数を優先**（JVM 実行で高速。RDD / Python UDF は Python プロセスで遅い）
# MAGIC - **処理するデータ量を減らす**（早めに `filter` / `select`）
# MAGIC - **小さいテーブルは broadcast join** でシャッフルを避ける
# MAGIC - **再利用する DataFrame は cache**、**パーティション数は repartition / coalesce** で調整
# MAGIC - **保存フォーマットは Parquet/Delta（列指向）**、データの偏り（Skew）に注意

# COMMAND ----------

from pyspark.sql.functions import broadcast

# 実行計画の確認（最適化の様子が見える）
df_joined.explain(mode="simple")

# broadcast join：小さい customer をブロードキャストして大きい orders と結合
df_bcast = df_order.join(broadcast(df_customer), df_order.o_custkey == df_customer.c_custkey, "inner")
print("broadcast join の計画（BroadcastHashJoin になる）:")
df_bcast.explain(mode="simple")

# 再利用する集計はキャッシュして再計算を避ける
seg_summary = df_joined.groupBy("c_mktsegment").agg(_sum("o_totalprice").alias("revenue")).cache()
print("キャッシュした集計件数:", seg_summary.count())  # 1回目で実体化
print("2回目はキャッシュから高速に返る:", seg_summary.count())
display(seg_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 pandas との相互変換と Apache Arrow
# MAGIC 集計後の小さな結果は pandas に変換して可視化・後処理できます。**Arrow** を有効にすると変換が高速化されます。
# MAGIC ※ `toPandas()` は全件をドライバーに集めるため、**小さく集計してから**変換します。

# COMMAND ----------

# Arrow を有効化（変換高速化）
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# 小さく集計してから pandas へ
pdf = (df_customer.groupBy("c_mktsegment")
       .agg(avg("c_acctbal").alias("avg_balance"))
       .toPandas())
print(type(pdf))
print(pdf)

# pandas DataFrame → Spark DataFrame に戻すこともできる
df_back = spark.createDataFrame(pdf)
display(df_back)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.9 UDF とそのコスト
# MAGIC 組み込み関数で書けない処理は **UDF** で実装できますが、Python UDF は Python プロセスで実行され
# MAGIC シリアライズのオーバーヘッドがあります。**まず組み込み関数・SQL 式で書けないか**を検討しましょう。

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 例：残高をランク文字列に変換する UDF（学習用。実務では expr/CASE 推奨）
@udf(returnType=StringType())
def balance_rank(bal):
    if bal is None:
        return "unknown"
    if bal > 5000:
        return "gold"
    if bal > 0:
        return "silver"
    return "negative"

display(df_customer.withColumn("rank", balance_rank("c_acctbal"))
        .select("c_custkey", "c_acctbal", "rank").limit(10))

# 同じ処理を組み込み式で（こちらが高速）
display(df_customer.withColumn("rank",
        expr("CASE WHEN c_acctbal > 5000 THEN 'gold' WHEN c_acctbal > 0 THEN 'silver' ELSE 'negative' END"))
        .select("c_custkey", "c_acctbal", "rank").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC **Part 1（初級）**
# MAGIC - Spark / PySpark の概念とアーキテクチャ（Py4J・JVM 実行・遅延評価）
# MAGIC - SparkSession、変換とアクションの違い、`explain` による実行計画の確認
# MAGIC - DataFrame の 4 つの作成方法、スキーマ（メタデータ）の確認
# MAGIC - 合成データ生成 → Delta テーブル保存・読み込み → 列・行操作 → データソース読み書き
# MAGIC
# MAGIC **Part 2（中級）**
# MAGIC - 結合・集計・メソッドチェーン、SQL 連携の 4 スタイル
# MAGIC - `collect` の作法、DataFrame in/out 設計と単体テスト
# MAGIC - 変数・クラス・Config 活用、スキーマに基づく動的処理・動的 SQL 生成
# MAGIC - パフォーマンス（broadcast join / cache / explain / Parquet）、pandas + Arrow、UDF とコスト
# MAGIC
# MAGIC ### 後片付け（任意）
# MAGIC 生成物を削除したい場合は次のセルのコメントを外して実行してください。

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{schema_name}` CASCADE")
print("完了 ✅ お疲れさまでした！")
