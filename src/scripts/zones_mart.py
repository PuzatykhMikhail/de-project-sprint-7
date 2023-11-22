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
from pyspark.sql.functions import expr
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
import utilities as u
import findspark

findspark.init()
findspark.find()

# date = '2022-05-31'
# depth = '30'


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
        .appName("zones")
        .getOrCreate()
    )


    udf_func = F.udf(u.get_distance)

    paths = u.input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)

    # города без timezone
    cities = (
        spark.read.csv(cities_path, sep=";", header=True)
        .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(F.DoubleType()))
        .withColumn("lon", F.regexp_replace("lng", ",", ".").cast(F.DoubleType()))
        .withColumnRenamed("lat", "lat_c")
        .withColumnRenamed("lon", "lon_c")
        .select("id", "city", "lat_c", "lon_c")  
    )
    # все сообщения
    all_messages = (
        events.where(F.col("event_type") == "message")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_id").alias("message_id"),
            "lat",
            "lon",
            F.to_date(F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))).alias("date"),
        )
    )

    # все реакции
    all_reactions = (
        events.where(F.col("event_type") == "reaction")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(
            F.col("event.message_id").alias("message_id"),
            "lat",
            "lon",
            F.to_date(F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))).alias("date"),
        )
    )

    # описания
    all_subscriptions = (
        events.where(F.col("event_type") == "subscription")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(
            F.col("event.user").alias("user_id"),
            "lat",
            "lon",
            F.to_date(F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))).alias("date"),
        )
    )

    # привязка сообщений к геоданным городов
    all_messages_geodata = all_messages.crossJoin(cities.hint("broadcast"))

    # привязка реакций к геоданным городов
    all_reactions_geodata = all_reactions.crossJoin(cities.hint("broadcast"))

    # привязка подписки к геоданным городов
    all_subscriptions_geodata = all_subscriptions.crossJoin(cities.hint("broadcast")
    )

    # подсчёт дистанции  сообщения
    distance_messages = all_messages_geodata.withColumn("distance",udf_func(F.col("lat"), F.col("lon"), F.col("lat_c"), F.col("lon_c")).cast(F.DoubleType()),)

    # подсчёт дистанции  реакции
    distance_reactions = all_reactions_geodata.withColumn("distance",udf_func(F.col("lat"), F.col("lon"), F.col("lat_c"), F.col("lon_c")).cast(F.DoubleType()),)

    # подсчёт дистанции  подписки
    distance_subscriptions = all_subscriptions_geodata.withColumn("distance",udf_func(F.col("lat"), F.col("lon"), F.col("lat_c"), F.col("lon_c")).cast(F.DoubleType()),)

    # ближайший город сообщения
    city_message = (
        distance_messages.withColumn("row",F.row_number().over(Window.partitionBy("user_id", "date", "message_id").orderBy(F.col("distance").asc())),)
        .filter(F.col("row") == 1)
        .select("user_id", "message_id", "date", "id", "city")
        .withColumnRenamed("city", "zone_id")
    )

    # ближайший город реакция
    city_reaction = (
        distance_reactions.withColumn("row",F.row_number().over(Window.partitionBy("message_id", "date", "lat", "lon").orderBy(F.col("distance").asc())),)
        .filter(F.col("row") == 1)
        .select("message_id", "date", "id", "city")
        .withColumnRenamed("city", "zone_id")
    )

    # ближайший город подписки
    city_subscription = (
        distance_subscriptions.withColumn("row",F.row_number().over(Window.partitionBy("lat", "lon", "date").orderBy(F.col("distance").asc())),)
        .filter(F.col("row") == 1)
        .select("user_id", "date", "id", "city")
        .withColumnRenamed("city", "zone_id")
    )

    # расчёт сообщений по неделям и месяцам
    count_messages = (
        city_message.withColumn("month", month(F.col("date")))
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd")))
        .withColumn("week_message",(F.count("message_id").over(Window.partitionBy("zone_id", "week"))),)
        .withColumn("month_message",(F.count("message_id").over(Window.partitionBy("zone_id", "month"))),)
        .select("zone_id", "week", "month", "week_message", "month_message")
        .distinct()
    )

     # расчёт регистрации по неделям и месяцам
    count_registrations = (
        city_message.withColumn("month", month(F.col("date")))
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd")))
        .withColumn("row",(F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("date").asc()))),)
        .filter(F.col("row") == 1)
        .withColumn("week_user", (F.count("row").over(Window.partitionBy("zone_id", "week"))))
        .withColumn("month_user", (F.count("row").over(Window.partitionBy("zone_id", "month"))))
        .select("zone_id", "week", "month", "week_user", "month_user")
        .distinct()
    )

    # расчёт реакций  по неделям и месяцам
    count_reactions = (
        city_reaction.withColumn("month", month(F.col("date")))
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd")))
        .withColumn("week_reaction",(F.count("message_id").over(Window.partitionBy("zone_id", "week"))),).withColumn("month_reaction",(F.count("message_id").over(Window.partitionBy("zone_id", "month"))),)
        .select("zone_id", "week", "month", "week_reaction", "month_reaction")
        .distinct()
    )

    # расчёт подписок по неделям и месяцам
    count_subscriptions = (
        city_subscription.withColumn("month", month(F.col("date")))
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd")))
        .withColumn("week_subscription",(F.count("user_id").over(Window.partitionBy("zone_id", "week"))),)
        .withColumn("month_subscription",(F.count("user_id").over(Window.partitionBy("zone_id", "month"))),)
        .select("zone_id", "week", "month", "week_subscription", "month_subscription")
        .distinct()
    )

    # финальный расчет
    geo_analitics_mart = (
        count_messages.join(count_registrations, ["zone_id", "week", "month"], how="full")
        .join(count_reactions, ["zone_id", "week", "month"], how="full")
        .join(count_subscriptions, ["zone_id", "week", "month"], how="full")
    )

    # Заполняем пропуски значением по умолчанию
    geo_analitics_mart = geo_analitics_mart.fillna(0)
    
    geo_analitics_mart.write.mode("overwrite").parquet(f"{target_path}/mart/zones")

if __name__ == "__main__":

    main()