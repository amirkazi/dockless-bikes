'''
ETL Pipeline for GBFS Feed for Dockless Bikes Pilot in Chicago.

Date: 23rd August 2018

Code Details:
    Scrapes GBFS data from JSON files, converts to format specified in 
    'https://github.com/dsgermain/gbfs/blob/
        f76251ad4c754b62defc42562887724f287b73ea/gbfs.md',
    cleans data where required, and creates dataframe(s) for uploading to 
    PostGRES database.
    Currently creates CSVs for specified dataframes.

'''


import pandas as pd
import urllib.request
import json
from datetime import datetime

#HELPER FUNCTIONS

def getting_json_data (link):
    '''
    Given a URL to a JSON webpage, downloads the content of the JSON webpage 
        as a dictionary
    
    Input: link (string)
    Output: dictionary
    
    '''
    data = json.load(urllib.request.urlopen(link))
    return data

def convert_to_dataframe(hierarchy):
    '''
    Given a dictionary, will convert it to a pandas dataframe
    
    Input: dictionary (from JSON)
    Output: Pandas dataframe
    '''
    df = pd.DataFrame(hierarchy)
    return df
    

def adding_checking_columns(column, old_dataframe, new_dataframe):
    '''
    Given a dataframe, and a colum name, checks if the column is present in 
        the dataframe.
        If it is, copies it to a new dataframe.
        If it isn't, makes a column with that name in the new dataframe, 
            and enters NaN as value.

    Input:
        column: string
        new_dataframe: dataframe
        old_dataframe: dataframe
    '''
    if column not in old_dataframe.columns:
        new_dataframe[column] = 'NaN'
    else:
        new_dataframe[column] = old_dataframe[column]


def adding_extras(dataframe, link, company_name):
    '''
    Given a dataframe, a JSON file link, and a string, 
        updated the dataframe with columns for last_updated, time_scraped,
        and company name.

    Input:
        dataframe
        link: JSON file
        company_name: string
    '''
    try:
        dataframe['last_updated'] = \
            datetime.fromtimestamp(int(link['last_updated'])).isoformat()
    except: 
        pass
    dataframe['time_scraped'] = str(datetime.now())
    dataframe['company_name'] = company_name


def running_functions(list_of_requirements, old_dataframe, link, company_name):
    '''
    For any list of columns listed in requirements, it checks if column  exists,
        adds the column to the new dataframe in the currect order, or adds NaN
        to it if it does not exist, and adds other columns such as company name,
        time_scraped and last_updated.

    Input:
        list_of_requirements: list
        old_dataframe: dataframe
        link: string to JSON file
        company_name: string

    Output:
        dataframe
    '''
    new_dataframe = pd.DataFrame()
    for item in list_of_requirements:
        adding_checking_columns(item, old_dataframe, new_dataframe)
    adding_extras(new_dataframe, link, company_name)
    
    return new_dataframe


def converting_to_csv(company_name, dataframe, file_name):
    '''
    Converts a dataframe to CSV, adds the company name, file name and time the
        file is made to the dataframe.
    '''
    csv_name = company_name + '__'+ file_name + '__' + str(datetime.now()) + '.csv'
    dataframe.to_csv(csv_name)
    

#FREE BIKE STATUS

def obtaining_free_bike_status(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe(link['data']['bikes'])
    
    requirements = ['bike_id', 'lat', 'lon', 'is_reserved', 'is_disabled',
        'is_ebike', 'station_id', 'geofence_zone_id']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#STATION INFORMATION

def obtaining_station_information(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe(link['data']['stations'])
    
    requirements = ['station_id', 'name', 'short_name', 'lat', 'lon', 'address',
        'cross_street', 'region_id', 'post_code', 'rental_methods', 'capacity']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#STATION STATUS

def obtaining_station_status(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link['data']['stations'])
    
    requirements = ['station_id', 'num_bikes_available', 
        'num_bikes_available_types', 'mechanical', 'ebike', 'num_bikes_disabled'
        , 'num_docks_available','num_docks_disabled', 'is_installed', 
        'is_renting', 'is_returning', 'last_reported', 'is_charging_station']      
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#SYSTEM INFORMATION

def obtaining_system_information(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.
    Calls a function to alter and drop certain data not required, and transpose
        the original dataframe.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link)
    
    requirements = ['system_id', 'language', 'name', 'short_name', 
        'operator', 'url', 'purchase_url', 'start_date', 'phone_number', 
        'email', 'timezone', 'license_url']
    
    temp_dataframe = rotating_system_information(temp_dataframe)
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


def rotating_system_information(system_information):
    '''
    Given a dataframe, drops the 'ttl' column and 'last_updated' column,
        and transposes the dataframe.
    '''
    system_information = system_information.drop(['ttl'], axis = 1)
    system_information =system_information.drop(['last_updated'], axis = 1)
    system_information = system_information.T
    
    return system_information


#GEOFENCING ZONE STATUS

def obtaining_geofencing_zone_status(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link['geofencing_zones'])
    
    requirements = ['geofencing_zone_id', 'num_bikes_available', 
        'num_bikes_available_types', 'mechanical', 'ebike', 
        'num_bikes_disabled', 'is_returning']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#SYSTEM HOURS

def obtaining_system_hours(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link['data']['rental_hours'])
    
    requirements = ['user_types','days', 'start_time', 'end_time']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#SYSTEM CALENDAR

def obtaining_system_calendar(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link['data']['calendars'])
    
    requirements = ['start_month','start_day', 'start_year', 'end_month', 
        'end_day', 'end_year']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#SYSTEM REGIONS

def obtaining_system_regions(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link['data']['regions'])
    
    requirements = ['region_id', 'name']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#SYSTEM PRICING PLANS

def obtaining_system_pricing_plans(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link['data']['plans'])
    
    requirements = ['plan_id','url','name', 'currency', 'price', 
        'is_taxable', 'description']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#SYSTEM ALERTS

def obtaining_system_alerts(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link['data']['alerts'])
    
    requirements = ['alert_id', 'type', 'times', 'start', 'end', 'station_ids', 
        'region_ids','url', 'summary', 'description', 'last_updated']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


#GEOFENCING ZONE INFORMATION

def obtaining_geofencing_zone_information(company_name, link):
    '''
    Given a company name, and a link,
        calls functions to create a dataframe with the specified requirements.

    Input:
        company_name: string
        link: string to JSON filee
    '''
    link = getting_json_data(link)
    temp_dataframe = convert_to_dataframe (link['geofencing_zones'])
    
    requirements = ['geofencing_zone_id', 'name', 'lat', 'lon', 'region_id', 
        'post_code', 'capacity', 'zone_area', 'station_id']
    
    return  running_functions (requirements, temp_dataframe, link, company_name)


def main():
    '''
    Commands for creating dataframes and for converting to CSV
    #### HIDDEN FOR CONFIDENTIALITY
    '''


if __name__ == '__main__':
    main()
