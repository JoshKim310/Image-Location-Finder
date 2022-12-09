import sys
import numpy as np
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



#Schema for the different amenity data
data_schema = types.StructType([
    types.StructField('lat', types.DoubleType(), nullable=False),
    types.StructField('lon', types.DoubleType(), nullable=False),
    types.StructField('city', types.StringType(), nullable = False),
    types.StructField('rarity', types.IntegerType(), nullable = False),
    types.StructField('amenity', types.StringType(), nullable=False),
    types.StructField('popularity', types.IntegerType(), nullable=False),
])



def variable_name(name, globals_dict):
    
    return [var_name for var_name in globals_dict if globals_dict[var_name] is name]

def main(osm):


    
    #load up vancouver data and store relevant info into variables
    allVanData = spark.read.csv(osm, schema=data_schema )



    atmData = allVanData.filter(allVanData['amenity'] == 'atm')
    bankData = allVanData.filter(allVanData['amenity'] == 'bank')
    collegeData = allVanData.filter(allVanData['amenity'] == 'college')
    ferryData = allVanData.filter(allVanData['amenity'] == 'ferry_terminal')
    libraryData = allVanData.filter(allVanData['amenity'] == 'library')
    policeData = allVanData.filter(allVanData['amenity'] == 'police')
    pubData = allVanData.filter(allVanData['amenity'] == 'pub')
    schoolData = allVanData.filter(allVanData['amenity'] == 'school')
    theatreData = allVanData.filter(allVanData['amenity'] == 'theatre')
    dentistData = allVanData.filter(allVanData['amenity'] == 'dentist')
    fastFoodData = allVanData.filter(allVanData['amenity'] == 'fast_food')
    parkingData = allVanData.filter(allVanData['amenity'] == 'parking')
    pharmacyData = allVanData.filter(allVanData['amenity'] == 'pharmacy')

    arrayOfData = [atmData, bankData, collegeData, ferryData, libraryData, policeData, pubData, schoolData, theatreData, dentistData, fastFoodData, parkingData, pharmacyData]
    
    
    cityData = allVanData.filter(allVanData['popularity'] > 20)

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