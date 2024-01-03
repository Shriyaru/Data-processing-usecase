# Data-processing-usecase
Requirement:
Write a python3.7+ program which would run locally on the laptop and perform the following:
#1 Generation phase: 
program should generate N=~5000 JSON files on disk in /tmp/flights/%MM-YY%-%origin_city%-flights.json or similar folder structure where each file is a JSON array of random size M = [10 – 100] of randomly generated flights data between cities. 
Total set of cities is K=[50-100].  Flight record is an object containing {date, origin city, destination city, flight duration time, # of passengers of board}. Some records, with probability L = [0.5% - 0.1%] have NULL in any of the flight record properties. 
 
#2 analysis/cleaning phase
Program should process those files and produce the following result: 
-          #count of total records processed, #count of dirty records and total run duration. 
-          AVG and P95 (95th percentile) of flight duration for Top 10 destinations. 
-          Assuming cities had originally 0 passengers, find two cities with MAX passengers arrived and left. 


For the above use case I have followed the below steps:
#1 Generation phase:
1.  Generated the fake data using faker library by considering the Null values and other things, for have stored the data in flight_data.csv file, as in local system was not able to generate more data the pc was crashing, whereas did not use Azure Cloud as it was all client tenants. Whereas in pyspark we can use rdd, also in adf we have optoins for parallel processing in the activities.
2.	Stored the cleaned data into the csv file where also added few columns in flight_data_processed.csv .
3.  Read the flight_data_processed.csv file and created the 1200 plus json files by grouping the data on year, month and city. Stored the json file in the given path ie. tmp/flights .
4.  To read the json file and process the same have used ThreadPoolExecutor, for paralleling processing all the json files at a time.
Flight data to be processed and split the data into multiple json files, and process the json files, for cleaning and analysis phase.

#2 analysis/cleaning phase
1. Handled Null values, for columns where there is no date, origin_city and destination_city have dropped the rows.
2. Handled unrelevant data like when on same date, the data shows the flight's origin and destination is same have dropped those rows as well.
3. Also have filled the pssengers count with the mean values if there is any nulls.

Please install the below libraries before running the code:

•	pip install numpy
•	pip install pandas
