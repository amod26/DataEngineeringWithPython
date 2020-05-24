# Capstone Project 

- We will be parsing data from a csv file containing the demographics of different races in 50 states of America. <br>
- After parsing, we will model the data into different postgres facts & dim tables to be used for analytical purpose.


## Schema Design <br> 

### Fact table

- demograph: (city,median_age,male_population,female_population,total_population,no_of_veterans,foreign_born,avg_household_size,race,count)  <br>

### Dimension table

- city: (state_code,states,city)

### Files:

- create_tables.py
- sql_queries.py
- etl.py 
- us-cities-demographics.csv

### To Run:
1. create_tables.py
2. etl.py 

