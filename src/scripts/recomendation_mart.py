import sys
import datetime
import math

import pyspark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark.context import SparkContext

import findspark

import utilities as u

findspark.init()
findspark.find()



def main():


    sc = SparkContext()
    sql = SQLContext(sc)

    date = sys.argv[1]
    depth = sys.argv[2]
    events_path = sys.argv[3]
    cities_path = sys.argv[4]
    target_path = sys.argv[5]

    

    spark = (
        SparkSession.builder.master("yarn")
        .appName("recomendations")
        .getOrCreate()
    )

    udf_func = F.udf(u.get_distance)

    cities = (
        spark.read.csv(cities_path, sep=";", header=True)
        .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(F.DoubleType()))
        .withColumn("lon", F.regexp_replace("lng", ",", ".").cast(F.DoubleType()))
        .withColumnRenamed("lat", "lat_c")
        .withColumnRenamed("lon", "lon_c")
        .select("id", "city", "lat_c", "lon_c", "timezone")
    )

    paths = u.input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)

    # все подписки
    all_subscriptions = (
        events.filter(F.col("event_type") == "subscription")
        .where(
            (
                F.col("event.subscription_channel").isNotNull()
                & F.col("event.user").isNotNull()
            )
        )
        .select(
            F.col("event.subscription_channel").alias("channel_id"),
            F.col("event.user").alias("user_id"),
        )
        .distinct()
    )

    # отправители и получатели
    cols = ["user_left", "user_right"]
    subscriptions = (
        all_subscriptions.withColumnRenamed("user_id", "user_left")
        .join(all_subscriptions.withColumnRenamed("user_id", "user_right"),on="channel_id",how="inner",)
        .drop("channel_id")
        .filter(F.col("user_left") != F.col("user_right"))
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
    )

    # отправители
    users_sender = (
        events.filter("event_type == 'message'")
        .where(
            (
                F.col("event.message_from").isNotNull()
                & F.col("event.message_to").isNotNull()
            )
        )
        .select(
            F.col("event.message_from").alias("user_left"),
            F.col("event.message_to").alias("user_right"),
            F.col("lat").alias("lat_from"),
            F.col("lon").alias("lon_from"),
        )
        .distinct()
    )

    # получатели
    users_receiver = (
        events.filter("event_type == 'message'")
        .where(
            (
                F.col("event.message_from").isNotNull()
                & F.col("event.message_to").isNotNull()
            )
        )
        .select(
            F.col("event.message_to").alias("user_left"),
            F.col("event.message_from").alias("user_right"),
            F.col("lat").alias("lat_to"),
            F.col("lon").alias("lon_to"),
        )
        .distinct()
    )

    # unix формат сообщения
    events_messages_unix = (
        events.filter("event_type == 'message'")
        .where(
            F.col("lat").isNotNull()
            | (F.col("lon").isNotNull())
            | (F.unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").isNotNull())
        )
        .select(
            F.col("event.message_from").alias("user_right"),
            F.col("lat").alias("lat"),
            F.col("lon").alias("lon"),
            F.unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").alias("time"),
        )
        .distinct()
    )

    w = Window.partitionBy("time")

    # последние сообщения
    lasts_messages = (
        events_messages_unix.withColumn("maxdatetime", F.max("time").over(w))
        .where(F.col("time") == F.col("maxdatetime"))
        .drop("maxdatetime")
    )

    # unix формат подписки
    events_subscription_unix = (
        events.filter("event_type == 'subscription'")
        .where(
            F.col("lat").isNotNull()
            | (F.col("lon").isNotNull())
            | (F.unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").isNotNull())
        )
        .select(
            F.col("event.message_from").alias("user_right"),
            F.col("lat"),
            F.col("lon"),F.unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").alias("time"),
        )
        .distinct() 
    )

    # последние подписки
    lasts_subscription = (
        events_subscription_unix.withColumn("maxdatetime", F.max("time").over(w))
        .where(F.col("time") == F.col("maxdatetime"))
        .drop("maxdatetime")
    )

    # подписки и сообщения
    events_coordinates = lasts_subscription.union(lasts_messages).distinct()

    # пересечение пользователей
    users_intersection = (
        users_sender.union(users_receiver)
        .withColumn("arr", F.array_sort(F.array(*cols)))
        .drop_duplicates(["arr"])
        .drop("arr")
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
        .filter(F.col("user_left") != F.col("user_right"))
    )

    # не пересекающиейся пользователи
    subscriptions_without_intersection = (
        subscriptions.join(
            users_intersection.withColumnRenamed("user_right", "user_right_temp")
            .withColumnRenamed("user_left", "user_left_temp"),on=["hash"],how="left",
        )
        .where(F.col("user_right_temp").isNull())
        .drop("user_right_temp", "user_left_temp", "hash")
        .where(F.col("user_left") != 0)
        .filter(F.col("user_left") != F.col("user_right"))
    )

    # последние координаты
    lasts_subscription_coordinates = subscriptions_without_intersection.join(
        events_coordinates.withColumnRenamed("user_id", "user_left")
        .withColumnRenamed("lon", "lon_left")
        .withColumnRenamed("lat", "lat_left"),on=["user_right"],how="inner",
    ).join(
        events_coordinates.withColumnRenamed("user_id", "user_right")
        .withColumnRenamed("lon", "lon_right")
        .withColumnRenamed("lat", "lat_right"),
        on=["user_right"],
        how="inner",
    )

    # не пересекающиейся пользователи в одном городе
    distance = (
        lasts_subscription_coordinates.withColumn(
            "distance",
            udf_func(
                F.col("lat_left"),
                F.col("lat_right"),
                F.col("lon_left"),
                F.col("lon_right"),
            ).cast(F.DoubleType()),
        )
        .where(F.col("distance") <= 1.0)
        .withColumnRenamed("lat_left", "lat")
        .withColumnRenamed("lon_left", "lon")
        .drop("lat_from", "lon_from", "distance")
    )

    # город
    users_city = (
        distance.crossJoin(cities.hint("broadcast"))
        .withColumn("distance",udf_func(F.col("lon"), F.col("lat"), F.col("lon_c"), F.col("lat_c")).cast(F.DoubleType()),)
        .withColumn("row",F.row_number().over(Window.partitionBy("user_left", "user_right").orderBy(F.col("distance").asc())),)
        .filter(F.col("row") == 1)
        .drop("row", "lon", "lat", "city_lon", "city_lat", "distance", "channel_id")
        .withColumnRenamed("city", "zone_id")
        .distinct()
    )

    # финальный расчет
    recommendations = (
        users_city.withColumn("processed_dttm", current_date())
        .withColumn("local_datetime",F.from_utc_timestamp(F.col("processed_dttm"), F.col("timezone")),)
        .withColumn("local_time", date_format(col("local_datetime"), "HH:mm:ss"))
        .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")
    )

    recommendations.write.mode("overwrite").parquet(f"{target_path}/mart/recommendations/")


if __name__ == "__main__":

    main()