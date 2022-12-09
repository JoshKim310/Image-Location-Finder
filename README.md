# CMPT 353 Project

Description

## Table of Contents

* [Installation](#installation)
* [Usage](#usage)
  * [Generating the Data](#generating-the-data)
  * [Running the Analysis](#running-the-analysis)


## Installation

Run requirements.txt file

```
pip install -r requirements.txt
```

## Usage

### Generating the Data

** **Note: Pre generated data is in the `generated-data` directory** **  

All of our data is created from the provided `amenities-vancouver.json.gz` and data  
from [Geoapify](https://www.geoapify.com/) using their [Reverse Geocoding api](https://www.geoapify.com/reverse-geocoding-api).

There are `two` scripts that generate our data:`create-data.py` and `data.py`.

### `create-data.py`

The `create-data.py` takes 3 command line input argument which are:  
argument 1: `amenities-vancouver.json.gz`  
argument 2: `amenity name from list below`  
argument 3: `amenity name appended by _data`

** **Note: These amenities have a count from 10-37 and is recommended to run these amenities for testing. (Amenities with a high count will increase the `create-data.py` execution time due to the increase in api calls. Full list of amenities are in ``)** **

<details>
    <summary>List of amenity names:</summary>
    <p>
        ferry_terminal  
        trolley_bay  
        prep_school  
        college  
        bureau_de_change  
        police  
        bicycle_repair_station  
        vacuum_cleaner  
        clock  
        music_school  
        social_centre  
        compressed_air  
        bus_station  
        fire_station  
        marketplace  
        motorcycle_parking  
        taxi  
        food_court  
        parking_space  
        nightclub  
        shower  
        arts_centre  
        bbq  
        events_venue  
        boat_rental  
        cinema  
        research_institute  
        university  
        loading_dock  
        weighbridge  
    </p>
</details>


Example command for `create-data.py`:  
```
spark-submit create-data.py police police_data
```

### `combine-data.py`

Example command for `combine-data.py`:  
```
spark-submit combine-data.py amenities-vancouver.csv
```

---

### Running the Analysis

There are ` ` scripts that generate our analyses: `predict-image.py`.  

### `predict-image.py`

The `predict-image.py` file takes in an image as input and predicts where the image was taken.

The `predict-image.py` takes 2 command line input arguments which are:  
argument 1: `.jpg file`  
argument 2: `amenities-vancouver.csv`  

** **Note: sample input images can be found in the `sample-images` directory** **

Example command for `predict-image.py`:
```
spark-submit predict-image.py IMG_8590.jpg amenities-vancouver.csv
```

---

### Generating Visualizations

There are `2` scripts that generate visuals: `create-map.py` and `make-graph.py`

### `create-map.py`

The `create-map.py` plots points on a map giving a visual of the distribution of the amenities.

The `create-map.py` takes 1 command line input argument which is:  
argument 1: `csv from generated-data`

Example command for `create-map.py`:  
for windows:  
```
python create-map.py .\generated-data\bank_data\part-00000-0133636f-dfa8-40a0-9c43-4ce07c07c1bd-c000.csv
```

for mac:  
```
python create-map.py ./generated-data/bank_data/part-00000-0133636f-dfa8-40a0-9c43-4ce07c07c1bd-c000.csv
```
