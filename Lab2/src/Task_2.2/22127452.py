# 22127452 - Task 2.2
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

filename = "Amazon Sale Report.csv"

spark = SparkSession.builder.appName("SKU weekly report").getOrCreate()

df = spark.read.csv(filename, header=True, inferSchema=True)
df2 = df.withColumn("Date", F.to_date(F.col("Date"), "MM-dd-yy"))

date_range = df.agg(F.min("Date").alias("start_date"), F.max("Date").alias("end_date")).collect()
start_date, end_date = date_range[0]["start_date"], date_range[0]["end_date"]
start_date = F.to_date(F.lit(start_date), "MM-dd-yy")
end_date = F.to_date(F.lit(end_date), "MM-dd-yy")

date_seq = spark.range(1).select(F.explode(F.sequence(start_date, end_date, F.expr("INTERVAL 1 DAY")))\
                                 .alias("report_date")) \
                                 .where(F.dayofweek("report_date") == 2)

sku_dates = df.select("sku").distinct().crossJoin(date_seq)

sales_with_mondays = sku_dates.alias("sku_dates") \
                            .join(df2.alias("sales"), sku_dates.sku == df2.SKU, "left")\
                            .where("sales.Date BETWEEN date_add(sku_dates.report_date, -6) and sku_dates.report_date") \
                            .select("sku_dates.sku", "sku_dates.report_date", "sales.Qty")

final_df = sales_with_mondays.groupBy("report_date", "sku")\
                            .agg(F.sum("qty").alias("total_quantity"))\
                            .fillna(0)\
                            .orderBy("report_date", "sku")\
                            .withColumn("report_date", F.date_format("report_date", "dd/MM/yyyy"))

final_df.write.csv("output", header=True, mode="overwrite")

spark.stop()
