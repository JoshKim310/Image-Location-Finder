from http import request
import requests
from requests.structures import CaseInsensitiveDict

#Libraries not used in class
from PIL import Image as PILimg
from PIL.ExifTags import TAGS
from exif import Image
from sqlalchemy import null



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

base = "https://api.geoapify.com/v1/geocode/reverse?lat=49.219983&lon=-122.988481&type=city&format=json&apiKey=afd572b5a5414cc58c98b25e8ce47fb7"



base_url = "https://api.geoapify.com/v1/geocode/reverse?lat=&lon=&apiKey=afd572b5a5414cc58c98b25e8ce47fb7"

    #gets lat and long coords of photo
imgCoords = image_coordinates('IMG_8590.jpg')
if (not imgCoords):
    print("Error")

lat = str(imgCoords[0])
lon = str(imgCoords[1])

coord_url = base_url[:48] + lat +base_url[48] + base_url[49:53] + lon + base_url[53:]


resp = requests.get(base)

