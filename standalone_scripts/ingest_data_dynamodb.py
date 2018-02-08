#!/bin/env python
import json, requests, logging, argparse, boto3
from threading import Thread
from datetime import datetime
from decimal import Decimal


def write_to_dynamodb(json):
    logging.info("Writing data to dynamodb")
    #setup connection to AWS
    #set profile name accordingly - refers to .aws/credentials
    session = boto3.session.Session(profile_name='cbuto', region_name='us-east-1')
    dynamodb = session.resource('dynamodb')
    #use CrimeReports table
    # key = OBJECTID
    # sort = Reported_Timestamp
    table = dynamodb.Table('CrimeReports')
    
    #batch write each year to dynamo
    with table.batch_writer() as batch:
        for record in json:
            #if geometry is not in record
            #set to 'null'
            if "geometry" not in record:
                batch.put_item(
                    Item={
                        'OBJECTID': record['attributes']['OBJECTID'],
                        'Reported_Timestamp': record['attributes']['Reported_Timestamp'],
                        'lat': 'null',
                        'long': 'null',
                        'OccurredFrom_Timestamp': record['attributes']['OccurredFrom_Timestamp'],
                        'OccurredThrough_Timestamp': record['attributes']['OccurredThrough_Timestamp'],
                        'Geocode_Street': record['attributes']['Geocode_Street'],
                        'Address_City': record['attributes']['Address_City'],
                        'Address_State': record['attributes']['Address_State'],
                        'Statute_CrimeCategory': record['attributes']['Statute_CrimeCategory'],
                        'Statute_Degree': record['attributes']['Statute_Degree'],
                        'Statute_Text': record['attributes']['Statute_Text'],
                        'Statute_Description': record['attributes']['Statute_Description'],
                        'Weapon_Description': record['attributes']['Weapon_Description'],
                        'Larceny_Type': record['attributes']['Larceny_Type'],
                        'Location_Type': record['attributes']['Location_Type'],
                    }
                )
            else:
                #geometry is in record - use x, y coordinates
                batch.put_item(
                    Item={
                        'OBJECTID': record['attributes']['OBJECTID'],
                        'Reported_Timestamp': record['attributes']['Reported_Timestamp'],
                        'lat': Decimal(str(record['geometry']['y'])),
                        'long': Decimal(str(record['geometry']['x'])),
                        'OccurredFrom_Timestamp': record['attributes']['OccurredFrom_Timestamp'],
                        'OccurredThrough_Timestamp': record['attributes']['OccurredThrough_Timestamp'],
                        'Geocode_Street': record['attributes']['Geocode_Street'],
                        'Address_City': record['attributes']['Address_City'],
                        'Address_State': record['attributes']['Address_State'],
                        'Statute_CrimeCategory': record['attributes']['Statute_CrimeCategory'],
                        'Statute_Degree': record['attributes']['Statute_Degree'],
                        'Statute_Text': record['attributes']['Statute_Text'],
                        'Statute_Description': record['attributes']['Statute_Description'],
                        'Weapon_Description': record['attributes']['Weapon_Description'],
                        'Larceny_Type': record['attributes']['Larceny_Type'],
                        'Location_Type': record['attributes']['Location_Type'],
                    }
                )
def query_RPD_api(start, end):
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

def get_year_data(year):
    """ A function to get RPD data for a year and write it to dynamodb
    :input year - the start of the range you are looking to capture """
    response = query_RPD_api(year, year + 1)
    write_to_dynamodb(response['features'])

def main():
    """ Main Function to loop through each year """ 
    #
    for i in range(2011, datetime.now().year):
        get_year_data(i)

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

    