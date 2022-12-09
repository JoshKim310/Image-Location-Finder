from calendar import c
import sys
import numpy as np
import pandas as pd
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import StandardScaler
from sqlalchemy import false, null, true
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('The data').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
spark.conf.set("spark.sql.session.timeZone", "UTC")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions, types




#Schema for the data we were given about greater Vancouver (Prof Baker provided)
amenity_schema = types.StructType([
    types.StructField('lat', types.DoubleType(), nullable=False),
    types.StructField('lon', types.DoubleType(), nullable=False),
    types.StructField('timestamp', types.TimestampType(), nullable=False),
    types.StructField('amenity', types.StringType(), nullable=False),
    types.StructField('name', types.StringType(), nullable=True),
    types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])

#Schema for the different amenity data
data_schema = types.StructType([
    types.StructField('lat', types.DoubleType(), nullable = False),
    types.StructField('lon', types.DoubleType(), nullable = False),
    types.StructField('amenity', types.StringType(), nullable=False),
    types.StructField('city', types.StringType(), nullable=False),
])



def amenity_filter(amenity):
    
    if (amenity == 'atm' or amenity == 'bank'or amenity == 'college' or amenity == 'ferry_terminal' or amenity == 'library' or amenity == 'police' 
    or amenity == 'pub' or amenity == 'school' or amenity == 'theatre' or amenity == 'dentist' or amenity == 'fast_food' or amenity == 'parking'
    or amenity  == 'pharmacy'):
        return True
    else:
        return False

amenity_udf = functions.udf(amenity_filter, returnType=types.BooleanType())


def main(osm):


    
    #load up vancouver data and store relevant info into variables
    allVanData = spark.read.json(osm, schema= amenity_schema)

    allVanData = allVanData.select('lat', 'lon', 'amenity')
    allVanData = allVanData.cache()
    amenityType = allVanData.groupBy('amenity').count()
    amenityType = amenityType.sort(amenityType['count'].desc())
    amenityType = amenityType.withColumnRenamed('count', 'rarity')
    amenityType = amenityType.coalesce(1)
    amenityType = amenityType.cache()
    allVanData = allVanData.join(amenityType, ['amenity'])
    allVanData = allVanData.withColumn("filter", amenity_udf(allVanData['amenity']))
    filteredData = allVanData.filter(allVanData['filter'] == True)


    atmData = spark.read.csv('generated-data/atm_data', schema=data_schema)
    bankData = spark.read.csv('generated-data/bank_data', schema=data_schema)
    collegeData = spark.read.csv('generated-data/college_data', schema=data_schema)
    ferryData = spark.read.csv('generated-data/ferry_terminal_data', schema=data_schema)
    libraryData = spark.read.csv('generated-data/library_data', schema=data_schema)
    policeData = spark.read.csv('generated-data/police_data', schema=data_schema)
    pubData = spark.read.csv('generated-data/pub_data', schema=data_schema)
    schoolData = spark.read.csv('generated-data/school_data', schema=data_schema)
    theatreData = spark.read.csv('generated-data/theatre_data', schema=data_schema)
    dentistData = spark.read.csv('generated-data/dentist_data', schema = data_schema)
    fastFoodData = spark.read.csv('generated-data/fast_food_data', schema = data_schema)
    parkingData = spark.read.csv('generated-data/parking_data', schema = data_schema)
    pharmacyData = spark.read.csv('generated-data/pharmacy_data', schema = data_schema)

    
    gatheredData = atmData.union(bankData).union(collegeData).union(ferryData).union(libraryData).union(policeData).union(pubData)\
    .union(schoolData).union(theatreData).union(dentistData).union(fastFoodData).union(parkingData).union(pharmacyData)

    filteredData = filteredData.join(gatheredData, ['lat', 'lon', 'amenity'])

    cityData = filteredData.groupBy('city').count()
    cityData = cityData.withColumnRenamed("count", "popularity")
    filteredData = filteredData.join(cityData, ['city'])
    filteredData = filteredData.select('lat', 'lon', 'city', 'rarity', 'amenity', 'popularity')
    filteredData = filteredData.coalesce(1)
    
    filteredData.write.csv('amenities-vancouver', mode='overwrite')

    


    
    

    
if __name__ == '__main__':
    osmData = sys.argv[1]
    main(osmData)