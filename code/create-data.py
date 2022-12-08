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

def create_csv(osmData, amenity, output):
    vanData = spark.read.json(osmData, schema=amenity_schema)
     
    selected = vanData.select('lat', 'lon', 'amenity')
    filtered = selected.filter(selected['amenity'] == amenity)

    # get count of amenity
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

    addCity = addCity.coalesce(1)
    addCity.write.csv(output, mode = 'overwrite')

def main(osmData):
    #create_csv(osmData, 'bank', 'bank_data')
    #create_csv(osmData, 'school', 'school_data')
    #create_csv(osmData, 'parking', 'parking_data')**
    #create_csv(osmData, 'atm', 'atm_data')
    #create_csv(osmData, 'pub', 'pub_data')
    #create_csv(osmData, 'library', 'library_data')
    #create_csv(osmData, 'theatre', 'theatre_data')
    #create_csv(osmData, 'college', 'college_data')
    #create_csv(osmData, 'police', 'police_data')
    #create_csv(osmData, 'ferry_terminal', 'ferry_terminal_data')
    #create_csv(osmData, 'dentist', 'dentist_data')**
    #create_csv(osmData, 'pharmacy', 'pharmacy_data')**
    return
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('The data').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')

    osmData = sys.argv[1]
    main(osmData)