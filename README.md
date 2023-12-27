# Data-processing-usecase
Flight data to be processed and split the data into multiple json files, and process the json files, for cleaning and analysis phase.

I have used Faker library to create the dummy data into the csv file with count of 5000, and later processed the csv file and splitted the data into multiple json files on the basis of the unique origin_city.

Below are the out puts that has been generated:
1.    #count of total records processed, #count of dirty records and total run duration. 
2.	AVG and P95 (95th percentile) of flight duration for Top 10 destinations. 
3.	Assuming cities had originally 0 passengers, find two cities with MAX passengers arrived and left. 

Please install the below libraries before running the code:

•	pip install numpy
•	pip install pandas
