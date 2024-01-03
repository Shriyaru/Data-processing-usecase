import pandas as pd
from datetime import datetime
import numpy as np
import time
import json
import os
import concurrent.futures
import csv

#Define a custom function to concatenate hours and minutes excluding NaN values
def concat_hours_minutes(source_json_data):
    if pd.notna(source_json_data['hours']) and pd.notna(source_json_data['minutes']):
        return f"{int(source_json_data['hours'])}:{int(source_json_data['minutes'])}"
    else:
        return np.nan


def create_json_files(source_data,output_dir):
    # Specify the directory to save JSON files
    os.makedirs(output_dir, exist_ok=True)

    #creating list of unique origin city to create the json files with the city name
    # Extract month and year from 'date' column


    # Group by year month and city to create the file name
    grouped_data = source_data.groupby(['year', 'month', 'origin_city'])

    for (year, month, origin_city), group in grouped_data:

        # Create a unique filename based on year, month, and cities
        filename = f"{month:02d}-{year}_{origin_city}.json"

        # Drop unnecessary columns before saving as it was created just for the filename
        group = group.drop(['month', 'year'], axis=1)

        # Save the group as a JSON file
        group.to_json(os.path.join(output_dir, filename), orient='records', lines=True,default_handler=str)

    print(f"{len(grouped_data)} JSON files created.")

# Function to process the json file line by line and append the file data in
def process_json_file(file_path):
    data_df = []

    with open(file_path, 'r') as file:
        for line in file:
            try:
                json_data = json.loads(line)

                df = pd.json_normalize(json_data)
                data_df.append(df)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in file {file_path}, line: {line.strip()}: {e}")

    # Concatenate DataFrames obtained from each line in the JSON file
    result_df = pd.concat(data_df, ignore_index=True)

    return result_df


# Function to process all JSON files in a folder in parallel using threadpool which will run
def process_all_json_files_parallel(json_folder):
    json_files = [os.path.join(json_folder, filename) for filename in os.listdir(json_folder) if
                  filename.endswith(".json")]
    #run the json files parallely using Threadpoolexecutor with 10 worker nodes
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Process each JSON file in parallel
        dfs = list(executor.map(process_json_file, json_files))

    # Concatenate DataFrames obtained from each JSON file
    json_dataframe = pd.concat(dfs, ignore_index=True)

    #converting the total_minutes column to int
    json_dataframe['total_minutes'] = json_dataframe['total_minutes'].astype(int)

    ######################## Total Number of records ################
    # getting total no of records processed
    total_number_of_records = len(json_dataframe)

    ########################## Total Null Records ####################
    total_null_records_count = json_dataframe.isnull().sum().sum()

    ######################## Logic for dirty records ################
    # Count of dirty records where at same date the flight's origin and destination city is same

    same_origin_dest_date_records = json_dataframe[json_dataframe['origin_city'] == json_dataframe['dest_city']]

    #filtering the dataframe where the origin and destination city is not same on the same date
    filtered_json_data = json_dataframe[json_dataframe['origin_city'] != json_dataframe['dest_city']]

    ######################## Logic to get the top two cities with max passengers ################
    # calculating the top two cities with Max passengers, for which below steps are followed.
    # created a unique list of destination cities to check the list of destination city is there in the origin city

    unique_dest_city = filtered_json_data['dest_city'].unique()
    arr_dep_data = filtered_json_data[filtered_json_data['origin_city'].isin(unique_dest_city)]

    # Grouped the data of origin city and passengers to get the total of the passengers per origin city
    grouped_data = arr_dep_data.groupby(['origin_city'])['passengers'].sum()

    # Get the top cities with Max passengers who arrived and left the destination
    top_2_cities_max_passengers = grouped_data.nlargest(2)

    # Calculating the avg and 95th percentile of top 10 destinations of flight duration
    total_flight_duration = filtered_json_data.groupby('dest_city')['total_minutes'].sum()

    top_10_destinations = total_flight_duration.nlargest(10).index
    top_10_df = filtered_json_data[filtered_json_data['dest_city'].isin(top_10_destinations)]

    # Calculate the average total minutes for the top 10 destinations over total minutes
    average_flight_duration_top_10 = top_10_df['total_minutes'].mean()
    #conveting minutes to hours and minutes
    avg_hours = average_flight_duration_top_10 // 60
    avg_remaining_minutes = average_flight_duration_top_10 % 60
    # Calculate the p95 over total minutes
    p95_flight_duration_top_10 = np.percentile(top_10_df['total_minutes'], 95)
    p95_hours = p95_flight_duration_top_10 // 60
    p95_remaining_minutes = p95_flight_duration_top_10 % 60

    return {
        'total_number_of_records': total_number_of_records,
        'total_null_records_count': total_null_records_count,
        'dirty_record_counts': len(same_origin_dest_date_records),
        'average_flight_duration_top_10': f"{average_flight_duration_top_10:.3f}",
        'average_flight_duration_top_10_in_hours_minutes': f"{int(avg_hours)} hours {avg_remaining_minutes :.2f} minutes",
        'p95_flight_duration_top_10': p95_flight_duration_top_10,
        'p95_flight_duration_top_10_in_hours_minutes': f"{p95_hours} hours {p95_remaining_minutes:.2f} minutes",
        'top_two_arrived_left_cities': top_2_cities_max_passengers
    }

if __name__ == "__main__":
    # created a variable to get the start time
    start_time = time.time()

    source_data = pd.read_csv("flight_dataset_custom_counts_per_date_new_8.csv")
    #finding the total number of records read from the file
    source_data_len = len(source_data)

    #dropped the rows which were having null values for the columns date, origin city and destination city
    source_cleaned_data: pd.DataFrame = source_data.dropna(subset=['date','origin_city','dest_city'])

    dropped_null_row_count = source_data_len - source_cleaned_data.shape[0]

    #if in case the passengers are 0 for any of the city then finding out the mean of origin and destination city and filling it with the mean values
    avg_passengers_by_route = source_cleaned_data.groupby(['origin_city', 'dest_city'])['passengers'].mean()

    # Fill null values in the passengers column with the corresponding average for the route
    source_cleaned_data['passengers'].fillna(source_cleaned_data.apply(
        lambda row: avg_passengers_by_route.get((row['origin_city'], row['dest_city']), None) if pd.isnull(
            row['passengers']) else None, axis=1
    ), inplace=True)

    #converting the date format to calculate the difference and time as while storing .
    source_cleaned_data['departure_datetime'] = pd.to_datetime(source_cleaned_data['departure_date_time'],format="%Y-%m-%d %H:%M")
    source_cleaned_data['arrival_datetime'] = pd.to_datetime(source_cleaned_data['arrival_date_time'],format="%Y-%m-%d %H:%M")

    source_cleaned_data['difference'] = source_cleaned_data['arrival_datetime'] - source_cleaned_data['departure_datetime']
    source_cleaned_data['total_minutes'] = source_cleaned_data['difference'].dt.total_seconds() / 60


    # Calculating the hours and minutes on the basis of the datetime difference calculated
    source_cleaned_data['hours'] = source_cleaned_data['difference'].dt.components['hours']
    source_cleaned_data['minutes'] = source_cleaned_data['difference'].dt.components['minutes']

    # Create a new column 'time_combined' to combine hours and minutes just to show
    source_cleaned_data['time_combined'] = source_cleaned_data.apply(concat_hours_minutes, axis=1)


    source_cleaned_data['date'] = pd.to_datetime(source_cleaned_data['date'],format="%Y-%m-%d %H:%M")

    source_cleaned_data['month'] = source_cleaned_data['date'].dt.month
    source_cleaned_data['year'] = source_cleaned_data['date'].dt.year

    # dropping the columns from the json file as it is not needed
    source_cleaned_data.drop(columns=['difference', 'arrival_datetime', 'departure_datetime', 'date'], inplace=True)

    output_dir = 'tmp/flights/'

    #stored the cleaned data in the csv file
    source_cleaned_data.to_csv('flight_data_processed.csv', index=False)

    # read the cleaned csv file
    cleaned_source_data = pd.read_csv("flight_data_processed.csv")

    # function to process the csv file and create the json files in the folders
    create_json_files(cleaned_source_data, output_dir)

    # the below function process all the json file parallely using the threadpoolexecutor
    result = process_all_json_files_parallel(output_dir)

    endtime = time.time()
    total_duration_to_process = endtime - start_time

    total_number_of_records = result['total_number_of_records'] - result['dirty_record_counts']

    print(f"Total no of records from source- {source_data_len}")
    print(f"Total dropped Null rows which has no arrival_date and departure_date - {dropped_null_row_count}")
    print(f"Total dirty record counts for same origin city and destination city on same day - {result['dirty_record_counts']}")
    print(f"Total no of records processed after cleaning- {total_number_of_records}")
    print(f"Total number of Null records row column - {result['total_null_records_count']}")
    print(f"Total Time taken to process the data -  {total_duration_to_process}")
    print(f"Average of Flight duration for top 10 destinations - {result['average_flight_duration_top_10']}")
    print(f"Average of Flight duration for top 10 destinations in hours and minutes - {result['average_flight_duration_top_10_in_hours_minutes']}")
    print(f"95th percentile of Flight duration for top 10 destinations - {result['p95_flight_duration_top_10']}")
    print(f"95th percentile of Flight duration for top 10 destinations in hours and minutes - {result['p95_flight_duration_top_10_in_hours_minutes']}")
    print(f"Top two cities with MAX passengers arrived and left-  {result['top_two_arrived_left_cities']}")