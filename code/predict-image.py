import sys
import numpy as np
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.tree import DecisionTreeClassifier
from sqlalchemy import null
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('The data').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
spark.conf.set("spark.sql.session.timeZone", "UTC")
from pyspark.sql import SparkSession, functions, types

#Libraries not used in class
from PIL import Image as PILimg
from PIL.ExifTags import TAGS
from exif import Image


data_schema = types.StructType([
    types.StructField('lat', types.DoubleType(), nullable=False),
    types.StructField('lon', types.DoubleType(), nullable=False),
    types.StructField('city', types.StringType(), nullable = False),
    types.StructField('rarity', types.IntegerType(), nullable = False),
    types.StructField('amenity', types.StringType(), nullable=False),
    types.StructField('popularity', types.IntegerType(), nullable=False),
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


def main(photoPath, osm):
    

    #gets lat and long coords of photo
    imgCoords = image_coordinates(photoPath)
    if (not imgCoords):
        print("Error")
        return


    
    #load up vancouver data and store relevant info into variables
    allVanData = spark.read.csv(osm, schema= data_schema)
    

    coordsVanData = np.array(allVanData.select('lat', 'lon').collect())
    cityVanData = np.array(allVanData.select('city').collect())

    X_train, X_valid, y_train, y_valid = train_test_split(coordsVanData, cityVanData)
    X_input = np.array([imgCoords])

    model_best = VotingClassifier([
        ('NaiveBayes', GaussianNB()),
        ('KNeighbors', KNeighborsClassifier()),
        ('DecisionTree', DecisionTreeClassifier()),
        ('RandomForest', RandomForestClassifier()),
        ('MLP', MLPClassifier()),
    ])

    model_best.fit(X_train, y_train.ravel())
    print("Training data score:",model_best.score(X_train, y_train))
    print("Valid data score:",model_best.score(X_valid, y_valid))
    print("Predicted city:",model_best.predict(X_input)[0])



    
    
    
    

    
if __name__ == '__main__':
    imagePath = sys.argv[1]
    osmData = sys.argv[2]
    main(imagePath, osmData)