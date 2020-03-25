#!/usr/bin/env python

import elasticsearch
import csv
import argparse
import os
import string
import geopy
import time
from functools import lru_cache
from geopy.geocoders import Nominatim

def strip_non_printable(s):
    return "".join(filter(lambda x: x in string.printable, s))


geolocator = Nominatim(user_agent="spectric_load_covid")
lookups = {}
def get_latlon(place):
    if place in lookups:
        return lookups.get(place)
    else:
        time.sleep(5)        
        location = geolocator.geocode(place)
        lookups[place] = location
        print(place, location)
        return location

parser = argparse.ArgumentParser()
parser.add_argument("-e", "--elasticsearch", default="http://localhost:9200")
parser.add_argument("-d", "--data", default="csse_covid_19_data/csse_covid_19_daily_reports")
parser.add_argument("-i", "--index", default="csse_covid_19_data")
parser.add_argument("-c", "--clear-all", default=False, action="store_true")
args = parser.parse_args()

es = elasticsearch.Elasticsearch(args.elasticsearch)

if args.clear_all:
    print("clearing all data")
    try:
        es.indices.delete(index=args.index)
    except elasticsearch.exceptions.NotFoundError:
        pass

fields = {
    "FIPS" : { "type" : "keyword" },
    "Admin2" : { "type" : "keyword" },
    "Province_State" : { "type" : "keyword" },
    "Country_Region" : { "type" : "keyword" },
    "Last_Update" : { "type" : "date", "format": "M/d/y H:m||yyyy-MM-dd HH:mm:ss||strict_date_optional_time" },
    "Location" : { "type" : "geo_point" },
    "Lat" : { "type" : "double" },
    "Lon" : { "type" : "double" },
    "Confirmed" : { "type" : "long" },
    "Deaths" : { "type" : "long" },
    "Recovered" : { "type" : "long" },
    "Active" : { "type" : "long" },
    "Combined_Key" : { "type" : "text" },
    "File" : { "type": "keyword"},
}

es.indices.create(
    index=args.index,
    body={
        "mappings" : {
            "properties" : fields
        }
    }
)

print("loading all data")
files = os.listdir(args.data)
for f in files:
    f = os.path.join(args.data, f)
    
    try:
        es.delete_by_query(
            index=args.index,
            body={ 'query': { 'term': { 'File': f } } }
        )
    except elasticsearch.exceptions.NotFoundError:
        pass


    with open(f) as fd:
        rows = csv.DictReader(fd)
        # TODO use bulk operations
        for ii, row in enumerate(rows):
            for k in list( row.keys() ):
                row[strip_non_printable(k)] = row.pop(k)

            row["File"] = f

            if row.get("Last Update") is not None:
                row["Last_Update"] = row.pop("Last Update")
            if row.get("Latitude") is not None:
                row["Lat"] = row.pop("Latitude")
            if row.get("Longitude")  is not None:
                row["Lon"] = row.pop("Longitude")
            if row.get("Long_")  is not None:
                row["Lon"] = row.pop("Long_")
            if row.get("Country/Region") is not None:
                row["Country_Region"] = row.pop("Country/Region")
            if row.get("Province/State") is not None:
                row["Province_State"] = row.pop("Province/State")

            unmapped_fields = set(row.keys()) - set(fields.keys())
            if unmapped_fields:
                print("Unmapped fields", unmapped_fields, row, f)
                raise SystemExit

            if row.get("Lat") and row.get("Lon"):
                row["Location"] = {"lat": row["Lat"], "lon": row["Lon"]}

            if row.get("Location") is None:
                location = get_latlon(row["Province_State"])
                if location:
                    row["Location"] =  {"lat": location.latitude, "lon":location.longitude}

            try:
                es.index(
                    args.index,
                    row
                )
            except Exception as e:
                print("failed to load %s:%s due to %s" % (f, ii, e))
