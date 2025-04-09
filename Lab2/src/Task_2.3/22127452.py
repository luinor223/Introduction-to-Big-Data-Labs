# 22127452 - Task 2.3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, substring_index
from pyspark.sql.types import BooleanType

filename = "shapes.parquet"

spark = SparkSession.builder.appName("Detect overlap").getOrCreate()

df = spark.read.parquet(filename)

def isOverlap(vertices1, vertices2):
    Ax1 = min([v[0] for v in vertices1])
    Ax2 = max([v[0] for v in vertices1])
    Ay1 = min([v[1] for v in vertices1])
    Ay2 = max([v[1] for v in vertices1])
    Bx1 = min([v[0] for v in vertices2])
    Bx2 = max([v[0] for v in vertices2])
    By1 = min([v[1] for v in vertices2])
    By2 = max([v[1] for v in vertices2])
    if Ax1 >= Bx2 or Ax2 <= Bx1 or Ay1 >= By2 or Ay2 <= By1:
        return False
    return True

isOverlap_udf = udf(isOverlap, BooleanType())

df.alias("df1").crossJoin(df.alias("df2")) \
    .filter(isOverlap_udf(col("df1.vertices"), col("df2.vertices"))) \
    .withColumn("shape_1", substring_index(col("df1.shape_id"), "_", -1).cast("int")) \
    .withColumn("shape_2", substring_index(col("df2.shape_id"), "_", -1).cast("int")) \
    .select("shape_1", "shape_2") \
    .filter(col("shape_1") < col("shape_2")) \
    .orderBy("shape_1", "shape_2") \
    .write.csv("output", header=True, mode="overwrite")

spark.stop()