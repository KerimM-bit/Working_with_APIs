#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import requests
import simplejson as json
import psycopg2


# In[2]:



# In[5]:


connect1 = psycopg2.connect("dbname=postgis_programming user=postgres password=12345 host=localhost")
cursor1 = connect1.cursor()
cursor1.execute("SELECT gid, name, pop_2000, state FROM citiesx020 WHERE pop_2000 > 100000")
rows = cursor1.fetchall()
rows


# In[6]:


def GetWeatherData(lon, lat, key):
    """
       Getting  closest 10 weather data station from given point.
    """
    #I'll use this JSON formatted  url to get weather data.
    url = ('http://api.openweathermap.org/data/2.5/find?lat=%s&lon=%s&cnt=10&appid=%s'
          %(lat, lon, key))
    print("Accessing weather data: %s " % url)
    try:
        data = requests.get(url)
        print("Request stuation: %s " % data.status_code)
        js_data = json.loads(data.text)
        return js_data['list']
    except:
        print("Error occured while downloading the weather data.")
        print(sys.exc_info()[0])
        return []
def AddWeatherStation(station_id, lon, lat, name, temperature):
    """  If it is not exists add weather data to database """
    curws = conn.cursor()
    curws.execute('SELECT * FROM wstations WHERE id=%s' %station_id)
    count = curws.rowcount
    if count == 0: #adding weather station
        curws.execute("INSERT INTO wstations (id, the_geom, name, temperature) VALUES (%s, ST_GeomFromText('POINT(%s %s)', 4326), %s, %s)",
                    (station_id, lon, lat, name, temperature))
        
        curws.close()
        print('Added the %s weather station to the database.' % name)
        return True
    else: #Weather information already exists in database.
        print("The %s weather station is already in the database." % name)
        return False
        
    #Application starts here
    #Firstly we have to access to database.

    #we do  not need transaction here, so set the connection to autocommit mode
 
    #open cursor to update the table with weather data

    
    # iterate all of the cities in the cities PostGIS layer, and for each of them grab the actual temperaure from the
    #closest weather station and add the 10 closest stations to the city to the wstation PostGIS layer
    
conn = psycopg2.connect('dbname=postgis_programming user=postgres password=12345 host=localhost' )
conn.set_isolation_level(0)
cur = conn.cursor()
cur.execute(" SELECT gid, name, ST_X(the_geom) AS lon, ST_Y(the_geom) AS lat, pop_2000 FROM citiesx020 WHERE pop_2000 > 100000;")
for record in cur:
    gid = record[0]
    cities_name = record[1]
    lon = record[2]
    lat = record[3]
    stations = GetWeatherData(lon, lat, 'your api key')
    print(stations)
    for station in stations:
        print(station)
        station_id = station['id']
        name = station['name']
            #for weather data we need to acces the 'main section in the json 'main: 
            #{'pressure': 990, 'temp': 272.15, 'humidity':54}
         
        if 'main' in station:
            if 'temp' in station['main']:
                temperature = station['main']['temp']
            else:
                temperature = -9999
                    #in some cases temp is not available
                #'coords' section 
        station_lon = station['coord']['lon']
        station_lat = station['coord']['lat']
        #hava istasyonlarini veritabanina ekliyoruz
        AddWeatherStation(station_id, station_lon, station_lat, name, temperature)
            # first weather st from the json API response is always the closest to the city, so we are grabbing the temp
            #and store in the temperture field in cities PostGIS layer
        if station_id == stations[0]['id']:
            print('Setting temperature to %s for the city %s' %(temperature, cities_name))
            cur2 = conn.cursor()
            cur2.execute('UPDATE citiesx020 SET temperature=%s WHERE gid=%s' % (temperature, gid))
            cur2.close()
        #close cursor, comot and close conecction to database
cur.close()
conn.commit()
conn.close()   

