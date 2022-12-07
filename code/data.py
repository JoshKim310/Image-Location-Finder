from ctypes import sizeof
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import null
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

#Converts the coords into decimal coordinates
def decimal_coords(coords, ref):

    decimal_degrees = coords[0] + coords[1] / 60 + coords[2] / 3600
    if (ref == "S" or ref == "W"):
        decimal_degrees = -decimal_degrees
    return decimal_degrees


#Get the coordinates of the image by passing through the image path
def image_coordinates(img_path):
    coords = null
    #Uses the path of the image and exif extracts data from it
    with open(img_path, 'rb') as src:
        img = Image(src)

        #If image has exif data it prints it out, if not then that also gets printed out
        if img.has_exif:
            #Makes call to the function that calculates the longitude and latitude of image
            x = decimal_coords(img.gps_latitude, img.gps_latitude_ref)
            y = decimal_coords(img.gps_longitude, img.gps_longitude_ref)
            coords = (x, y)
            
        else:
            print("The image has no EXIF information")
            return
        print("Was taken:", img.datetime_original,"and has coordinates:",coords)
        return coords



def log_transform(input):
    if (input < 0):
        temp = math.log(-input)
        return -temp
    else:
        return math.log(input)

log_udf = functions.udf(log_transform, returnType=types.DoubleType())

def main(photoPath, osm):
    
    
    #gets lat and long coords of photo
    imgCoords = image_coordinates(photoPath)
    if (not imgCoords):
        print("Error")
        return
    
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

    
    coordsVanData = np.array(allVanData.select('lat', 'lon').collect())
    amenitiesVanData = np.array(allVanData.select('amenity').collect())

    

    X_train, X_valid, y_train, y_valid = train_test_split(coordsVanData, amenitiesVanData)
    model = make_pipeline(
        KNeighborsClassifier(n_neighbors=3)
        )
    model.fit(X_train, y_train)
    print(model.score(X_train, y_train))
    print(model.score(X_valid, y_valid))
    X_test = np.array([imgCoords])
    print(model.predict(X_test))

    
    

    
    

    
    """
    plt.figure(figsize=(15, 6))
    plt.plot(X, Y, '.b', alpha = 0.5)
    plt.plot(imgCoords[0], imgCoords[1], '.r' ,alpha = 0.8)
    plt.savefig('filtered_output.png')
    plt.close()
    """
    
    
    
    
        
    

    





if __name__ == '__main__':
    imagePath = sys.argv[1]
    osmData = sys.argv[2]
    main(imagePath, osmData)