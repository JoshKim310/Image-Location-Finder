import sys
import requests
import json
from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('The data').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')

amenity_schema = types.StructType([
    types.StructField('lat', types.DoubleType(), nullable=False),
    types.StructField('lon', types.DoubleType(), nullable=False),
    types.StructField('timestamp', types.TimestampType(), nullable=False),
    types.StructField('amenity', types.StringType(), nullable=False),
    types.StructField('name', types.StringType(), nullable=True),
    types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])

def main(osmData):
    vanData = spark.read.json(osmData, schema=amenity_schema)
    vanData.show()

    return

if __name__ == '__main__':
    osmData = sys.argv[1]
    main(osmData)