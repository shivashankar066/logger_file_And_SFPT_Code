import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Initialize geolocator
geolocator = Nominatim(user_agent="LocationAgent")

# Create a DataFrame
df = pd.read_excel(r'D:\LOGISTIC\KN-Output.xlsx')

# Function to fetch latitude and longitude using geopy
def get_lat_long(location):
    try:
        print(location)
        location = geolocator.geocode(location)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except GeocoderTimedOut:
        return None, None


# Cache to store previously fetched lat/long values
location_cache = {}

# Lists to store lat/long values for DataFrame
location_From_location_lat = []
location_From_location_long = []
location_To_location_lat = []
location_To_location_long = []
location_Departure_location_lat = []
location_Departure_location_long = []
location_Arrival_location_lat = []
location_Arrival_location_long = []
location_VIA_location_lat = []
location_VIA_location_long = []
Latitude = []
Longitude = []

# Iterate through DataFrame rows
for index, row in df.iterrows():
    loc_a = row['From location']
    loc_b = row['To location']
    loc_c = row['Departure location']
    loc_d = row['Arrival location']   # Final Arrival
    loc_e = row['VIA']
    loc_f = row['Current Location']

    if loc_a in location_cache:
        lat_a, long_a = location_cache[loc_a]
    else:
        lat_a, long_a = get_lat_long(loc_a)
        location_cache[loc_a] = (lat_a, long_a)

    if loc_b in location_cache:
        lat_b, long_b = location_cache[loc_b]
    else:
        lat_b, long_b = get_lat_long(loc_b)
        location_cache[loc_b] = (lat_b, long_b)

    if loc_c in location_cache:
        lat_c, long_c = location_cache[loc_c]
    else:
        lat_c, long_c = get_lat_long(loc_c)
        location_cache[loc_c] = (lat_c, long_c)

    if loc_d in location_cache:
        lat_d, long_d = location_cache[loc_d]
    else:
        lat_d, long_d = get_lat_long(loc_d)
        location_cache[loc_d] = (lat_d, long_d)

    if loc_e in location_cache:
        lat_e, long_e = location_cache[loc_e]
    else:
        lat_e, long_e = get_lat_long(loc_e)
        location_cache[loc_e] = (lat_e, long_e)

    if loc_f in location_cache:
        lat_f, long_f = location_cache[loc_f]
    else:
        lat_f, long_f = get_lat_long(loc_f)
        location_cache[loc_f] = (lat_f, long_f)

    location_From_location_lat.append(lat_a)
    location_From_location_long.append(long_a)
    location_To_location_lat.append(lat_b)
    location_To_location_long.append(long_b)
    location_Departure_location_lat.append(lat_c)
    location_Departure_location_long.append(long_c)
    location_Arrival_location_lat.append(lat_d)
    location_Arrival_location_long.append(long_d)
    location_VIA_location_lat.append(lat_e)
    location_VIA_location_long.append(long_e)
    Latitude.append(lat_f)
    Longitude.append(long_f)

# Add new columns to the DataFrame
df['From_location_lat'] = location_From_location_lat
df['From_location_long'] = location_From_location_long
df['To_location_lat'] = location_To_location_lat
df['To_location_long'] = location_To_location_long

df['Departure_location_lat'] = location_Departure_location_lat
df['Departure_location_long'] = location_Departure_location_long
df['Arrival_location_lat'] = location_Arrival_location_lat
df['Arrival_location_long'] = location_Arrival_location_long

df['VIA_location_lat'] = location_VIA_location_lat
df['VIA_location_long'] = location_VIA_location_long

df['Longitude']=Longitude
df['Latitude']=Latitude
df.to_excel("KN-Output_Final.xlsx")