"""
QUERY TOOL - Đọc kết quả từ HDFS bằng PySpark
================================================
Chạy sau khi hệ thống đã hoạt động một lúc để xem thống kê.

Cách dùng:
    python query_results.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI    = "hdfs://localhost:9000"
HDFS_OUTPUT = "/person_counter/results"


def main():
    spark = (
        SparkSession.builder
        .appName("PersonCounter-Query")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    path = HDFS_URI + HDFS_OUTPUT
    print(f"\nĐọc dữ liệu từ: {path}\n")

    df = spark.read.parquet(path)
    total_frames = df.count()

    print(f"{'─'*50}")
    print(f" TỔNG QUAN HỆ THỐNG ĐẾM NGƯỜI")
    print(f"{'─'*50}")
    print(f" Tổng số frames đã xử lý : {total_frames:,}")

    stats = df.agg(
        F.sum("person_count").alias("total_detections"),
        F.max("person_count").alias("max_persons"),
        F.avg("person_count").alias("avg_persons"),
        F.avg("process_ms").alias("avg_ms"),
    ).collect()[0]

    print(f" Tổng lượt phát hiện người: {int(stats['total_detections']):,}")
    print(f" Số người tối đa 1 frame  : {stats['max_persons']}")
    print(f" Trung bình người/frame   : {stats['avg_persons']:.2f}")
    print(f" Thời gian xử lý TB       : {stats['avg_ms']:.1f} ms")
    print(f"{'─'*50}")

    print("\n Top 10 frame có nhiều người nhất:")
    df.orderBy(F.desc("person_count")).select(
        "frame_id", "datetime_str", "person_count", "process_ms"
    ).show(10, truncate=False)

    print("\n Thống kê theo phút:")
    df.withColumn("minute", F.date_trunc("minute", F.to_timestamp("datetime_str"))) \
      .groupBy("minute") \
      .agg(
          F.count("frame_id").alias("frames"),
          F.avg("person_count").alias("avg_persons"),
          F.max("person_count").alias("peak_persons"),
      ) \
      .orderBy("minute") \
      .show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
