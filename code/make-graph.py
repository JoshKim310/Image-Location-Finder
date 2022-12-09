from calendar import c
from ctypes import sizeof
from fileinput import filename
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import make_pipeline
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
import math


#Libraries not used in class
from PIL import Image as PILimg
from PIL.ExifTags import TAGS
from exif import Image




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


def variable_name(name, globals_dict):
    
    return [var_name for var_name in globals_dict if globals_dict[var_name] is name]

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


    atmData = spark.read.csv('atm_data', schema=data_schema)
    bankData = spark.read.csv('bank_data', schema=data_schema)
    collegeData = spark.read.csv('college_data', schema=data_schema)
    ferryData = spark.read.csv('ferry_terminal_data', schema=data_schema)
    libraryData = spark.read.csv('library_data', schema=data_schema)
    policeData = spark.read.csv('police_data', schema=data_schema)
    pubData = spark.read.csv('pub_data', schema=data_schema)
    schoolData = spark.read.csv('school_data', schema=data_schema)
    theatreData = spark.read.csv('theatre_data', schema=data_schema)
    dentistData = spark.read.csv('dentist_data', schema = data_schema)
    fastFoodData = spark.read.csv('fast_food_data', schema = data_schema)
    parkingData = spark.read.csv('parking_data', schema = data_schema)
    pharmacyData = spark.read.csv('pharmacy_data', schema = data_schema)

    arrayOfData = [atmData, bankData, collegeData, ferryData, libraryData, policeData, pubData, schoolData, theatreData, dentistData, fastFoodData, parkingData, pharmacyData]
    
    gatheredData = atmData.union(bankData).union(collegeData).union(ferryData).union(libraryData).union(policeData).union(pubData)\
    .union(schoolData).union(theatreData).union(dentistData).union(fastFoodData).union(parkingData).union(pharmacyData)

    filteredData = filteredData.join(gatheredData, ['lat', 'lon', 'amenity'])

    cityData = filteredData.groupBy('city').count()
    cityData = cityData.withColumnRenamed("count", "popularity")
    filteredData = filteredData.join(cityData, ['city'])
    cityData = filteredData.filter(filteredData['popularity'] > 20)

    cityVancouver = cityData.filter(cityData['city'] == 'Vancouver')
    citySurrey = cityData.filter(cityData['city'] == 'Surrey')
    cityBurnaby = cityData.filter(cityData['city'] == 'Burnaby')
    cityAbbotsford = cityData.filter(cityData['city'] == 'Abbotsford')
    cityRichmond = cityData.filter(cityData['city'] == 'Richmond')
    cityDelta = cityData.filter(cityData['city'] == 'Delta')
    cityCoquitlam = cityData.filter(cityData['city'] == 'Coquitlam')
    cityLangley = cityData.filter(cityData['city'] == 'Township of Langley')
    cityNewWest = cityData.filter(cityData['city'] == 'New Westminster')
    cityWestVan = cityData.filter(cityData['city'] == 'West Vancouver')
    cityNorthVan = cityData.filter(cityData['city'] == 'District of North Vancouver')
    tempData = cityData.filter(cityData['city'] == 'North Vancouver')
    cityNorthVan = cityNorthVan.union(tempData)
    cityPoco = cityData.filter(cityData['city'] == 'Port Coquitlam')
    cityPomo = cityData.filter(cityData['city'] == 'Port Moody')
    cityRidge = cityData.filter(cityData['city'] == 'Maple Ridge')
    cityMission = cityData.filter(cityData['city'] == 'Mission')
    tempData = cityData.filter(cityData['city'] == 'City of Langley')
    cityLangley = cityLangley.union(tempData)
    cityAreaA = cityData.filter(cityData['city'] == 'Electoral Area A')
    cityWhiteRock = cityData.filter(cityData['city'] == 'White Rock')


    cityArray = [cityVancouver, citySurrey, cityBurnaby, cityAbbotsford, cityRichmond, cityDelta, cityCoquitlam, cityLangley,cityNewWest, cityWestVan,cityNorthVan,
    cityPoco, cityPomo, cityRidge, cityMission, cityAreaA, cityWhiteRock]

    colours = ['blue', 'red', 'green', 'magenta', 'orange', 'yellow', 'gray', 'brown', 'cyan', 'darkslategray', 'tan', 'olive', 'peru', 'teal', 'pink', 'indigo', 'lightcoral']
    plt.figure(figsize=(15, 6))
    plt.title('Map of amenities')
    plt.xlabel('Latitude')
    plt.ylabel('Longitude')
    globals_dict = locals()

    counter = 0
    for i in cityArray:
        X = np.array(i.select('lat').collect())
        Y = np.array(i.select('lon').collect())
        filename = variable_name(i, globals_dict)
        print(filename[0],"is now being plotted on graph")
        plt.scatter(X, Y, c=colours[counter], label=filename[0])
        counter+=1
    plt.legend(loc="upper right")
    plt.savefig('map_of_amenities.png')
    plt.close()



    for i in arrayOfData:
        X = np.array(i.select('lat').collect())
        Y = np.array(i.select('lon').collect())
        plt.figure(figsize=(15, 6))
        fileName = variable_name(i, globals_dict)
        print(fileName[0],"is now saving as image")
        plt.title(fileName[0])
        plt.xlabel("Latitude")
        plt.ylabel("Longitude")
        plt.scatter(X, Y, color = 'blue', s = 30)
        plt.savefig(fileName[0]+".png")
        plt.close()



    
    

    
if __name__ == '__main__':
    osmData = sys.argv[1]
    main(osmData)