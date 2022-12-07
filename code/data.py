import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('The data').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
spark.conf.set("spark.sql.session.timeZone", "UTC")
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



def main(photoPath):

    
    #Uses the path of the image and extracts data from it
    with open(photoPath, 'rb') as src:
        img = Image(src)

        #If image has exif data it prints it out, if not then that also gets printed out
        if img.has_exif:
            info = f"has the EXIF {img.exif_version}"
        else:
            info = "does not contain any EXIF information"
        print(f"Image {src.name}: {info}")
        

    

    image = PILimg.open(photoPath)
    print(image)
    
    #image_coordinates(photoPath)




if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)