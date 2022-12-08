import sys
import requests
import json
import pandas as pd
import numpy as np
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
    cities = []
    lat = np.array(filtered.select('lat').collect())
    lon = np.array(filtered.select('lon').collect())  


    
    for i in range(count.first()['count']):
        url = f"https://api.geoapify.com/v1/geocode/reverse?lat={lat[i][0]}&lon={lon[i][0]}&format=json&apiKey=afd572b5a5414cc58c98b25e8ce47fb7"
        cities.append(requests.get(url).json()['results'][0]['city'])
    

    panda_df = pd.DataFrame(cities, columns=['cities'])
    panda_df['lat'] = lat
    panda_df['lon'] = lon
    print(panda_df)
    df_cities = spark.createDataFrame(panda_df)

    addCity = filtered.join(df_cities, ['lat', 'lon'])
    
    addCity.write.csv('school_data', mode = 'overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('The data').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')

    osmData = sys.argv[1]
    main(osmData)