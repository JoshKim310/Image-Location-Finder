import sys
import requests
import json
from pyspark.sql import SparkSession, functions, types, Row


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
     
    selected = vanData.select('lat', 'lon', 'amenity')
    filtered = selected.filter(selected['amenity'] == 'school')

    # get count of schools
    count = filtered.groupBy('amenity').count()
    print(count.first()['count'])
    school = ['asfd' , 'asdf', 'as']

    #for i in range(count.first()['count']):
    #    url = "https://api.geoapify.com/v1/geocode/reverse?lat=49.3451932&lon=-123.1497994&format=json&apiKey=afd572b5a5414cc58c98b25e8ce47fb7"
    #    school[i] = requests.get(url).json()['results'][0]['city']
    
    df_cities = spark.createDataFrame(data=school, schema=['cities'])
    df_cities.show();return
    addCity = filtered.withColumn('city', functions.column('school'))
    
    addCity.show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('The data').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')

    osmData = sys.argv[1]
    main(osmData)