# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark ハンズオン on Databricks
# MAGIC
# MAGIC このノートブックは、Databricks 上で **PySpark** の基本操作をひと通り体験するためのハンズオンです。
# MAGIC 合成データの生成から始まり、DataFrame 操作・結合・集計・SQL 連携・各種データソースの読み書きまでを一気通貫で実行します。
# MAGIC
# MAGIC **参考ドキュメント**
# MAGIC - [Databricks の PySpark](https://docs.databricks.com/aws/ja/pyspark/)
# MAGIC - [PySpark の基本](https://docs.databricks.com/aws/ja/pyspark/basics)
# MAGIC - [PySpark のデータソース](https://docs.databricks.com/aws/ja/pyspark/datasources)
# MAGIC
# MAGIC **実行環境**: Serverless もしくは任意の汎用クラスター（Databricks Runtime 15.4 LTS 以降を推奨）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. パラメータ設定
# MAGIC ハンズオン用の出力先カタログ・スキーマをウィジェットで指定します。
# MAGIC 既定では書き込み可能なカタログにハンズオン専用スキーマを作成します。

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
# MAGIC ## 1. セットアップ（スキーマ・ボリューム作成）
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
# MAGIC ## 2. DataFrame の作成
# MAGIC まずは最も基本的な `spark.createDataFrame` で、値を直接指定して DataFrame を作成します。
# MAGIC PySpark の変換処理は **遅延評価** され、`display` や `count` などのアクションで初めて計算されます。

# COMMAND ----------

df_children = spark.createDataFrame(
    data=[("Mikhail", 15), ("Zaky", 13), ("Zoya", 8)],
    schema=["name", "age"],
)
display(df_children)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 合成データの生成（顧客・注文）
# MAGIC `spark.range` と `pyspark.sql.functions` を組み合わせて、分散処理に乗る形で大規模な合成データを生成します。
# MAGIC ループで 1 行ずつ作るのではなく、関数で列を一括生成するのが PySpark 流です。

# COMMAND ----------

from pyspark.sql import functions as F

segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
nations = ["JAPAN", "UNITED STATES", "GERMANY", "FRANCE", "INDIA", "BRAZIL"]

# 顧客マスタ
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

# 注文トランザクション（顧客に外部キーで紐付け）
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
# MAGIC ## 4. Delta テーブルとして保存
# MAGIC 生成した DataFrame を Unity Catalog のマネージド Delta テーブルとして保存します（`saveAsTable`）。

# COMMAND ----------

df_customer.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.customer")
df_order.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.orders")

print("保存済みテーブル:")
display(spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. テーブルの読み込み
# MAGIC `spark.table` で保存済みテーブルを DataFrame として読み込みます。

# COMMAND ----------

df_customer = spark.table(f"{catalog_name}.{schema_name}.customer")
df_order = spark.table(f"{catalog_name}.{schema_name}.orders")
print(f"customer 件数 : {df_customer.count():,}")
print(f"orders   件数 : {df_order.count():,}")
df_customer.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 列の操作（select / withColumn / cast / rename）

# COMMAND ----------

from pyspark.sql.functions import col

# 列の選択
display(df_customer.select(col("c_custkey"), col("c_name"), col("c_acctbal")))

# 新しい列の作成（条件フラグ）
df_customer_flag = df_customer.withColumn("balance_flag", col("c_acctbal") > 1000)
display(df_customer_flag.select("c_custkey", "c_acctbal", "balance_flag"))

# データ型の変換 と 列名の変更
df_casted = (
    df_customer
    .withColumn("c_custkey_str", col("c_custkey").cast("string"))
    .withColumnRenamed("c_acctbal", "account_balance")
)
df_casted.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 行の操作（filter / distinct / na / sort / limit）

# COMMAND ----------

# 単一条件・複数条件のフィルタ
display(df_customer.filter(col("c_custkey") == 42))
display(df_customer.filter((col("c_mktsegment") == "BUILDING") & (col("c_acctbal") > 5000)))

# 重複削除・NULL 処理
print("セグメント一覧:")
display(df_customer.select("c_mktsegment").distinct())

# ソートして上位 10 件
df_top10 = df_customer.sort(col("c_acctbal").desc()).limit(10)
display(df_top10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. DataFrame の結合（JOIN）
# MAGIC 注文と顧客を `o_custkey = c_custkey` で内部結合します。

# COMMAND ----------

df_joined = df_order.join(
    df_customer,
    on=df_order["o_custkey"] == df_customer["c_custkey"],
    how="inner",
)
display(df_joined.select("o_orderkey", "o_totalprice", "c_name", "c_mktsegment", "c_nation").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. データの集計（groupBy / agg）

# COMMAND ----------

from pyspark.sql.functions import avg, count, sum as _sum

# セグメント別の平均残高
display(
    df_customer.groupBy("c_mktsegment").agg(
        avg("c_acctbal").alias("avg_balance"),
        count("*").alias("n_customers"),
    ).sort(col("avg_balance").desc())
)

# 国 × セグメントの複数キー集計
display(
    df_customer.groupBy("c_nation", "c_mktsegment").agg(
        avg("c_acctbal").alias("avg_balance")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. メソッドチェーン
# MAGIC filter → groupBy → agg → sort を 1 つの式として読みやすく繋げます。

# COMMAND ----------

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
# MAGIC ## 11. SQL との連携
# MAGIC DataFrame を一時ビューに登録し、`spark.sql` で SQL を実行できます。PySpark と SQL は相互運用可能です。

# COMMAND ----------

df_joined.createOrReplaceTempView("joined_view")

df_sql = spark.sql("""
    SELECT c_nation,
           c_mktsegment,
           COUNT(*)            AS n_orders,
           ROUND(SUM(o_totalprice), 2) AS revenue
    FROM joined_view
    GROUP BY c_nation, c_mktsegment
    ORDER BY revenue DESC
    LIMIT 15
""")
display(df_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. データソース：各種フォーマットの書き込み・読み込み
# MAGIC ボリューム上に CSV / JSON / Parquet で書き出し、読み戻して件数を確認します。

# COMMAND ----------

# 集計結果をサンプルとして各フォーマットで書き出し
sample = df_sql

(sample.write.format("csv").option("header", True).mode("overwrite").save(f"{volume_path}/revenue_csv"))
(sample.write.format("json").mode("overwrite").save(f"{volume_path}/revenue_json"))
(sample.write.format("parquet").mode("overwrite").save(f"{volume_path}/revenue_parquet"))

print("書き出し完了:")
for f in dbutils.fs.ls(volume_path):
    print(" ", f.name)

# COMMAND ----------

# CSV の読み込み（ヘッダー付き・スキーマ推論）
df_csv = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{volume_path}/revenue_csv")
)
print("CSV 読み込み:")
display(df_csv)

# Parquet の読み込み（スキーマは自己記述）
df_parquet = spark.read.format("parquet").load(f"{volume_path}/revenue_parquet")
print(f"Parquet 件数: {df_parquet.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. まとめ
# MAGIC お疲れさまでした。本ハンズオンでは以下を実施しました。
# MAGIC
# MAGIC 1. `createDataFrame` による DataFrame 作成（遅延評価の確認）
# MAGIC 2. `spark.range` + 関数による合成データの分散生成
# MAGIC 3. Delta テーブルへの保存（`saveAsTable`）と読み込み（`spark.table`）
# MAGIC 4. 列操作（`select` / `withColumn` / `cast` / `withColumnRenamed`）
# MAGIC 5. 行操作（`filter` / `distinct` / `sort` / `limit`）
# MAGIC 6. 結合（`join`）・集計（`groupBy` + `agg`）・メソッドチェーン
# MAGIC 7. SQL 連携（`createOrReplaceTempView` + `spark.sql`）
# MAGIC 8. データソースの読み書き（CSV / JSON / Parquet on Volume）
# MAGIC
# MAGIC ### 後片付け（任意）
# MAGIC 生成物を削除したい場合は次のセルのコメントを外して実行してください。

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS `{catalog_name}`.`{schema_name}` CASCADE")
print("完了 ✅")
