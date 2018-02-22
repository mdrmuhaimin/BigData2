from pyspark import SparkConf, SparkContext
import sys
import operator
import json
import math
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from pyspark.sql.types import (
	StructField, StructType, FloatType, StringType
)

# Run with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 streaming_consumer.py  2>/dev/null

colummns = ['Time', 'r.ankle Acceleration X (m/s^2)',
       'r.ankle Acceleration Y (m/s^2)', 'r.ankle Acceleration Z (m/s^2)',
       'r.ankle Angular Velocity X (rad/s)',
       'r.ankle Angular Velocity Y (rad/s)',
       'r.ankle Angular Velocity Z (rad/s)', 'r.ankle Magnetic Field X (uT)',
       'r.ankle Magnetic Field Y (uT)', 'r.ankle Magnetic Field Z (uT)',
       'l.ankle Acceleration X (m/s^2)', 'l.ankle Acceleration Y (m/s^2)',
       'l.ankle Acceleration Z (m/s^2)', 'l.ankle Angular Velocity X (rad/s)',
       'l.ankle Angular Velocity Y (rad/s)',
       'l.ankle Angular Velocity Z (rad/s)', 'l.ankle Magnetic Field X (uT)',
       'l.ankle Magnetic Field Y (uT)', 'l.ankle Magnetic Field Z (uT)',
       'r.thigh Acceleration X (m/s^2)', 'r.thigh Acceleration Y (m/s^2)',
       'r.thigh Acceleration Z (m/s^2)', 'r.thigh Angular Velocity X (rad/s)',
       'r.thigh Angular Velocity Y (rad/s)',
       'r.thigh Angular Velocity Z (rad/s)', 'r.thigh Magnetic Field X (uT)',
       'r.thigh Magnetic Field Y (uT)', 'r.thigh Magnetic Field Z (uT)',
       'l.thigh Acceleration X (m/s^2)', 'l.thigh Acceleration Y (m/s^2)',
       'l.thigh Acceleration Z (m/s^2)', 'l.thigh Angular Velocity X (rad/s)',
       'l.thigh Angular Velocity Y (rad/s)',
       'l.thigh Angular Velocity Z (rad/s)', 'l.thigh Magnetic Field X (uT)',
       'l.thigh Magnetic Field Y (uT)', 'l.thigh Magnetic Field Z (uT)',
       'head Acceleration X (m/s^2)', 'head Acceleration Y (m/s^2)',
       'head Acceleration Z (m/s^2)', 'head Angular Velocity X (rad/s)',
       'head Angular Velocity Y (rad/s)', 'head Angular Velocity Z (rad/s)',
       'head Magnetic Field X (uT)', 'head Magnetic Field Y (uT)',
       'head Magnetic Field Z (uT)', 'sternum Acceleration X (m/s^2)',
       'sternum Acceleration Y (m/s^2)', 'sternum Acceleration Z (m/s^2)',
       'sternum Angular Velocity X (rad/s)',
       'sternum Angular Velocity Y (rad/s)',
       'sternum Angular Velocity Z (rad/s)', 'sternum Magnetic Field X (uT)',
       'sternum Magnetic Field Y (uT)', 'sternum Magnetic Field Z (uT)',
       'waist Acceleration X (m/s^2)', 'waist Acceleration Y (m/s^2)',
       'waist Acceleration Z (m/s^2)', 'waist Angular Velocity X (rad/s)',
       'waist Angular Velocity Y (rad/s)', 'waist Angular Velocity Z (rad/s)',
       'waist Magnetic Field X (uT)', 'waist Magnetic Field Y (uT)',
       'waist Magnetic Field Z (uT)', 'FileName', 'Subject', 'Trial Type']

# Have a hard-coded subject for now
topic = "trials-6"
 
# Start spark session
spark = SparkSession.builder.appName('read_stream').getOrCreate()
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+
 
def main() :
	# Obtain messages
	messages = spark.readStream.format('kafka') \
		.option('kafka.bootstrap.servers', 'localhost:9092') \
		.option('subscribe', topic).load()

	# Get the value of the messages
	lines  = messages.select(messages['value'].cast('string'))

	# Split on ","
	asLists = F.split(lines["value"], ",")

	# Add all the columns
	i = 0
	for col in colummns :
		lines = lines.withColumn(col,asLists.getItem(i))
		i += 1

	# Drop the "value" column
	lines = lines.drop("value")
	lines.createOrReplaceTempView('lines')

	# Get the query to print it out on console
	query = lines \
	.writeStream \
	.format("console") \
	.start()

	query.awaitTermination(600)


if __name__ == "__main__":
	main()
