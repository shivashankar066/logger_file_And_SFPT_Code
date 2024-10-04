import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Initialize geolocator
geolocator = Nominatim(user_agent="LocationAgent")

# Create a DataFrame
df = pd.read_excel(r'E:\Logistic_Dashboard\Control Tower.xlsx')

df=df.sort_values(by='Milestone Datetime',ascending=True)

df["Current_Status"]=df.groupby("Tracking Number")["Status"].transform('last')
df["Current Location"]=df.groupby("Tracking Number")["Milestone Current Location"].transform('last')
df['Start Date']=df.groupby("Tracking Number")["Milestone Datetime"].transform('first')
df['Final Date']=df.groupby("Tracking Number")["Milestone Datetime"].transform('last')

df['Via-2_location'] = None
df['Via-2_date'] = None
df['Via-1_location'] = None
df['Via-1_date'] = None


def update_via_columns(group):
    for i in range(len(group) - 1, -1, -1):
        if group.iloc[i]['Status'].lower() == 'arrived':
            group['Via-2_location'] = group.iloc[i]['Milestone Current Location']
            group['Via-2_date'] = group.iloc[i]['Milestone Datetime']
            break

    for i in range(len(group) - 1, -1, -1):
        if group.iloc[i]['Status'].lower() == 'departed':
            group['Via-1_location'] = group.iloc[i]['Milestone Current Location']
            group['Via-1_date'] = group.iloc[i]['Milestone Datetime']
            break

    return group


df = df.groupby('Tracking Number').apply(update_via_columns).reset_index(drop=True)

# Function to fetch latitude and longitude using geopy
def get_lat_long(location):
    try:
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
    loc_a = row['Origin']
    loc_b = row['Final Destination']
    loc_c = row['Via-1_location']
    loc_d = row['Via-2_location']   # Final Arrival
    # loc_e = row['To Location']
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

    # if loc_e in location_cache:
    #     lat_e, long_e = location_cache[loc_e]
    # else:
    #     lat_e, long_e = get_lat_long(loc_e)
    #     location_cache[loc_e] = (lat_e, long_e)

    if loc_f in location_cache:
        lat_f, long_f = location_cache[loc_f]
    else:
        print(loc_f)
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
    # location_VIA_location_lat.append(lat_e)
    # location_VIA_location_long.append(long_e)
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

# df['VIA_location_lat'] = location_VIA_location_lat
# df['VIA_location_long'] = location_VIA_location_long

df['Longitude']=Longitude
df['Latitude']=Latitude
df.to_excel("E:/Logistic_Dashboard/FEDEX_DHL_V1.xlsx")
