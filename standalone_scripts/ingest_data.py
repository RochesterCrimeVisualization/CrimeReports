#!/bin/env python
import json
import requests
import logging
from threading import Thread
from datetime import datetime
import argparse

def parse_crime_object(json):
    pass

def get_json_rochester(start, end):
    logging.info("Querying Rochester Crime API for {}-{}".format(start, end))
    """ A function to capture the data of crime statistics given a year span
    :input start - Start Year
    :input end - end year
    :return json result
    """
    endpoint = 'https://maps.cityofrochester.gov/arcgis/rest/services/RPD/RPD_Part_I_Crime/FeatureServer/3/'
    query    = 'query?where=Reported_Date_Year%20%3E%3D%20{}%20AND%20Reported_Date_Year%20%3C%3D%20{}&outFields=*&outSR=4326&f=json'.format(start, end)
    response = requests.get(endpoint + query)
    logging.debug(response.text)
    return json.loads(response.text)


def threadded_year(year):
    """ A function to thread the request for JSON data
    :input year - the start of the range you are looking to capture """
    response = get_json_rochester(year, year + 1)
    threads = []
    for crime in response['features']:
        t = Thread(target=parse_crime_object, args=[crime])
        threads.append(t)
        t.start()
        parse_crime_object(crime)
    
    for thread in threads:
        thread.join()

def main():
    """ Main Function to loop through each year and start its own thread """ 
    threads = []
    for i in range(2011, datetime.now().year):
        t = Thread(target=threadded_year, args=[i])
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A function to capture the json data for 2011 - Current Year')
    parser.add_argument('--debug', '-d', action='store_true', help="Enable Logging")
    results = parser.parse_args()

    if results.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Logging Enabled")
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    main()

    