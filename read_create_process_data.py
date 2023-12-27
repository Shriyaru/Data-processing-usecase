#read the csv file and split the data into multiple json files where 10-100 no of records per file of 5000 records
import pandas as pd
from datetime import datetime
import numpy as np
import time
import json
import os

#Define a custom function to concatenate hours and minutes excluding NaN values
def concat_hours_minutes(source_json_data):
    if pd.notna(source_json_data['hours']) and pd.notna(source_json_data['minutes']):
        return f"{int(source_json_data['hours'])}:{int(source_json_data['minutes'])}"
    else:
        return np.nan

def convert_to_int(x):
    try:
        return int(x)
    except:
        return np.nan


def create_json_files(source_data,output_dir):
    # Specify the directory to save JSON files
    os.makedirs(output_dir, exist_ok=True)

    #creating list of unique origin city to create the json files with the city name

    for origin_city in source_data['origin_city'].unique():
        # Select records for the current origin city
        city_records = source_data[source_data['origin_city'] == origin_city]
        json_file_path = os.path.join(output_dir, f'{origin_city}_output.json')
        city_records.to_json(json_file_path, orient='records', lines=True, date_format='iso', date_unit='s', default_handler=str, force_ascii=False)

    print(f"{len(source_data['origin_city'].unique())} JSON files created.")

def process_json_files(output_dir):
    #reading all the json files and loading them again in one dataframe
    print("########### Getting all the json files form the directory and storing it into one dataframe")
    # Get all JSON file names as a list
    json_file_names = [filename for filename in os.listdir(output_dir) if filename.endswith('.json')]
    combined_dfs = []
    for json_file_name in json_file_names:
        json_file_path = os.path.join(output_dir, json_file_name)
        df = pd.read_json(json_file_path, lines=True)
        combined_dfs.append(df)

    # Concatinating all the .json files into one DataFrame to process
    combined_json_df = pd.concat(combined_dfs, ignore_index=True)
    # print('combined_json_df',combined_json_df)


    #getting total no of records processed
    total_number_of_records = len(combined_json_df)

    #Count of dirty records
    total_null_records_count = combined_json_df.isnull().sum().sum()

    # Calculating the two cities with MAX passengers arrived and left
    total_passengers = combined_json_df.groupby(['origin_city', 'dest_city'])['passengers'].sum().reset_index()
    # print('total passengers',total_passengers)

    # Find two cities with the maximum number of passengers arrived and left
    max_passengers_arrived = total_passengers.groupby('dest_city')['passengers'].sum()
    max_passengers_left = total_passengers.groupby('origin_city')['passengers'].sum()


    top_two_arrived_cities = max_passengers_arrived.nlargest(2)
    top_two_left_cities = max_passengers_left.nlargest(2)

    total_flight_duration = combined_json_df.groupby('dest_city')['hours'].sum()
    top_10_destinations = total_flight_duration.nlargest(10).index
    top_10_df = combined_json_df[combined_json_df['dest_city'].isin(top_10_destinations)]

    # Calculate the average flight duration for the top 10 destinations
    average_flight_duration_top_10 = top_10_df['hours'].mean()
    # Calculate the p95 flight duration for the top 10 destinations
    p95_flight_duration_top_10 = np.percentile(top_10_df['hours'].fillna(0), 95)

    return {
        'total_number_of_records' : total_number_of_records,
        'total_null_records_count': total_null_records_count,
        'average_flight_duration_top_10' :average_flight_duration_top_10,
        'p95_flight_duration_top_10':p95_flight_duration_top_10,
        'top_two_arrived_cities' :top_two_arrived_cities,
        'top_two_left_cities' :top_two_left_cities
    }

if __name__ == "__main__":
    # created a variable to get the start time
    start_time = time.time()

    source_data = pd.read_csv("flight_data.csv")
    source_data_len = len(source_data)
    source_cleaned_data = source_data.dropna(subset=['date'])
    dropped_null_row_count = source_data_len - source_cleaned_data.shape[0]
    source_cleaned_data['departure_datetime'] = pd.to_datetime(source_cleaned_data['departure_date_time'], format="%Y-%m-%d %H:%M")
    source_cleaned_data['arrival_datetime'] = pd.to_datetime(source_cleaned_data['arrival_date_time'], format="%Y-%m-%d %H:%M")
    source_cleaned_data['difference'] = source_cleaned_data['arrival_datetime'] - source_cleaned_data['departure_datetime']
    source_cleaned_data['total_minutes'] = source_cleaned_data['difference'].dt.total_seconds() / 60

    # Calculating the hours and minutes on the basis of the datetime difference calculated
    source_cleaned_data['hours'] = source_cleaned_data['difference'].dt.components['hours']
    source_cleaned_data['minutes'] = source_cleaned_data['difference'].dt.components['minutes']

    # Create a new column 'time_combined' to combine hours and minutes
    source_cleaned_data['time_combined'] = source_cleaned_data.apply(concat_hours_minutes, axis=1)
    # dropping the columns from the json file as it is not needed
    source_cleaned_data.drop(columns=['difference', 'arrival_datetime', 'departure_datetime', 'date'], inplace=True)
    output_dir = 'tmp/flights/'
    #function to process the csv file and create the json files in the folders
    create_json_files(source_cleaned_data,output_dir)
    #function to clean and analyse the data
    result = process_json_files(output_dir)
    endtime = time.time()
    total_duration_to_process = endtime - start_time


    print(f"Total no of records from source- {source_data_len}")
    print(f"Total no of records processed after cleaning- {result['total_number_of_records']}")
    print(f"Total number of Null records row column - {result['total_null_records_count']}")
    print(f"Total dropped Null rows which has no arrival_date and departure_date - {dropped_null_row_count}")
    print(f"Total Time taken to process the data -  {total_duration_to_process}")
    print(f"Average of Flight duration for top 10 destinations - {result['average_flight_duration_top_10']}")
    print(f"95th percentile of Flight duration for top 10 destinations - {result['p95_flight_duration_top_10']}")
    print(f"City with MAX passengers arrived -  {result['top_two_arrived_cities']}")
    print(f"City with MAX passengers left - {result['top_two_left_cities']}")



