from pyspark.sql import SparkSession
from  pyspark.sql.functions import split,explode,col,from_json,expr,window,count,to_timestamp
from pyspark.sql.types import StructField,StringType,StructType,IntegerType,FloatType,TimestampType,ArrayType,DoubleType,LongType
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Streaming kafka as a source') \
        .master('local[*]') \
        .getOrCreate()

    kafka_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers","localhost:9092") \
                .option("subscribe","NoBroker") \
                .option("startingOffsets","earliest") \
                .load()

    kafka_df.printSchema()

    myschema = StructType([
            StructField("user_id", StringType()),
            StructField("property_id", StringType()),
            StructField("date", StringType()),
            StructField("event_type", StringType())
    ])

    value_df = kafka_df.select(from_json(col("value").cast("string"),myschema).alias("value"))
    select_df = value_df.select("value.user_id", "value.property_id", "value.date","value.event_type")

    interaction_df = select_df.filter(select_df.event_type == "interaction")

#1. Everytime we see from the stream that a user u has done 9 interactions, we target the user opt for a paid service
    user_count_df = interaction_df.groupBy("user_id").count()
    rename_df = user_count_df.withColumnRenamed("count","user_count")
    where_df = rename_df.where(rename_df.user_count == 9)

    paid_service_df = where_df.selectExpr("user_id as key",
                                           """to_json(named_struct('user_id',user_id,'user_count',user_count)) as value""")

# 2 Everytime we see from the stream that a property p has received more than 5 interactions, we
    # send a notification to the owner asking if the property is closed

    property_count_df = interaction_df.groupBy("property_id").count()
    p_rename_df = property_count_df.withColumnRenamed("count", "property_count")
    p_where_df = p_rename_df.where(p_rename_df.property_count == 5)

    property_key = p_where_df.selectExpr("property_id as key",
                                         """to_json(named_struct('property_id',property_id,'property_count',property_count)) as value""")

#3 Everytime we see a user u doing more than 10 interactions in a minute, we flag the user as possible broker.

    window_df = interaction_df.groupBy(col("user_id"),window("date", "1 minute")).agg(count("property_id").alias("count_of_property"))
    start_df = window_df.select(window_df.window.start.cast("string").alias("start"),
             window_df.window.end.cast("string").alias("end"), "user_id","count_of_property")

    count_filter_df = start_df.where(start_df.count_of_property == 10)
    count_filter_df.printSchema()

    count_target_df = count_filter_df.selectExpr("start as key",
                                           """to_json(named_struct('start',start,'end',end,'user_id',user_id,'count_of_property',count_of_property)) as value""")

    paid_service_notification = paid_service_df \
        .writeStream \
        .queryName("Paid Service Notification") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "target_user_notification") \
        .outputMode("update") \
        .option("checkpointLocation", "file:///home/saif/Desktop/checkpoint/dir1") \
        .start()

    property_count_notification = property_key\
        .writeStream \
        .queryName("Property is closed or not") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "property_closed_open_notification") \
        .outputMode("update") \
        .option("checkpointLocation", "file:///home/saif/Desktop/checkpoint/dir2") \
        .start()



    possible_broker_notification = count_target_df \
        .writeStream \
        .queryName("Possible Broker") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "broker_notification") \
        .outputMode("update") \
        .option("checkpointLocation", "file:///home/saif/Desktop/checkpoint/dir3") \
        .start()

    paid_service_notification.awaitTermination()
    property_count_notification.awaitTermination()
    possible_broker_notification.awaitTermination()






