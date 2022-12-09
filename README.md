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

** Note: Pre generated data is in the `generated-data` directory **  

All of our data is created from the provided `amenities-vancouver.json.gz` and data  
from [Geoapify](https://www.geoapify.com/) using their [Reverse Geocoding api](https://www.geoapify.com/reverse-geocoding-api)

There are two scripts that generate our data:
`create-data.py` and `data.py`

The `create-data.py` takes 3 input argument which are:  
argument 1: `amenities-vancouver.json.gz`  
argument 2: `amenity name from list below`
argument 3: `amenity name appended by _data`

** Note: These amenities have a count from 10-37 and is recommended to run these amenities for testing. (Amenities with a high count will increase the `create-data.py `) execution time due to the increase in api calls. **

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

### Running the Analysis