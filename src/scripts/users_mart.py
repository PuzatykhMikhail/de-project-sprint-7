import sys
import datetime
import math

import pyspark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf

import findspark

import utilities as u
findspark.init()
findspark.find()



def main():

    date = sys.argv[1]
    depth = sys.argv[2]
    events_path = sys.argv[3]
    cities_path = sys.argv[4]
    target_path = sys.argv[5]

    sc = SparkContext()
    sql = SQLContext(sc)

    spark = (
        SparkSession.builder.master("yarn")
        .appName("Learning DataFrames")
        .getOrCreate()
    )

    udf_func=F.udf(u.get_distance)

    paths = u.input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)



    # города
    cities = (
        spark.read.csv(cities_path, sep=";", header=True)
        .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(F.DoubleType()))
        .withColumn("lon", F.regexp_replace("lng", ",", ".").cast(F.DoubleType()))
        .withColumnRenamed("lat", "lat_c")
        .withColumnRenamed("lon", "lon_c")
        .select("id", "city", "lat_c", "lon_c", "timezone")
    )




    # все сообщения
    events_messages = (
        events.where(F.col("event_type") == "message")
        .withColumn(
            "date",F.date_trunc("day",F.coalesce(F.col("event.datetime"), F.col("event.message_ts")),
            ),
        )
        .selectExpr(
            "event.message_from as user_id",
            "event.message_id",
            "date",
            "event.datetime",
            "lat",
            "lon",
        )
    )

    # города с сообщениями
    messages_cities = (
    events_messages.crossJoin(cities)
    .withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lon_c")).cast(
            "float"
        ),
    )
    .withColumn(
        "distance_rank",
        F.row_number().over(
            Window().partitionBy(["user_id", "message_id"]).orderBy(F.asc("distance"))
        ),
    )
    .where("distance_rank == 1")
    .select ("user_id", "messages_id", "date", "datetime", "city", "timezone")
    )

    # 
    active_messages_cities = (
    events_messages.withColumn(
        "datetime_rank",
        F.row_number().over(
            Window().partitionBy(["user_id"]).orderBy(F.desc("datetime"))
        ),
    )
    .where("datetime_rank == 1")
    .orderBy("user_id")
    .crossJoin(cities)
    .withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lon_c")).cast(
            "float"
        ),
    )
    .withColumn(
        "distance_rank",
        F.row_number().over(
            Window().partitionBy(["user_id"]).orderBy(F.asc("distance"))
        ),
    )
    .where("distance_rank == 1")
    .select("user_id", F.col("city").alias("act_city"), "date", "timezone")
    ) 

    # изменения города отправки сообщения
    change_city = (
        messages_cities.withColumn(
            "max_date", F.max("date").over(Window().partitionBy("user_id"))
        )
        .withColumn("city_lag",F.lead("act_city", 1, "empty").over(Window().partitionBy("user_id").orderBy(F.col("date").desc())),)
        .filter(F.col("act_city") != F.col("city_lag"))
    )

    # адрес города 27 дней подряд
    home_city = (
        change_city.withColumnRenamed("city", "home_city")
        .withColumn("date_lag",F.coalesce(F.lag("date").over(Window().partitionBy("user_id").orderBy(F.col("date").desc())),F.col("max_date"),),)
        .withColumn("date_diff", F.datediff(F.col("date_lag"), F.col("date")))
        .where(F.col("date_diff") > 27)
        .withColumn("rank",F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("date").desc())),)
        .where(F.col("rank") == 1)
        .drop("date_diff", "date_lag", "max_date", "city_lag", "rank")
    )

    # количество путешествий(смен города)+список городов
    travel_list = change_city.groupBy("user_id").agg(
        F.count("*").alias("travel_count"),
        F.collect_list("city").alias("travel_array")
    )

    # местное время
    time_local = active_messages_cities.withColumn(
        "localtime", F.from_utc_timestamp(F.col("date"), F.col("timezone"))
    ).drop("timezone", "city", "date", "datetime", "act_city")

    # финальная витрина
    final = (
        active_messages_cities.select("user_id", "act_city")
        .join(home_city, "user_id", "left")
        .join(travel_list, "user_id", "left")
        .join(time_local, "user_id", "left")
        .select(
            "user_id",
            "act_city",
            "home_city",
            "travel_count",
            "travel_array",
            "localtime",
        )
    )

    final.write.mode("overwrite").parquet(
        f"{target_path}/mart/users/_{date}_{depth}"
    )


if __name__ == "__main__":

    main()