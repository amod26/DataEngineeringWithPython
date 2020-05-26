# DROP TABLES

city_table_drop = "drop table if exists city_info;"
demograph_table_drop = "drop table if exists demograph;"

# CREATE TABLES

city_table_create = ("""create table if not exists city_info 
(
city varchar PRIMARY KEY,
state_code varchar,
states varchar
);
""")


demograph_table_create = ("""create table if not exists demograph
(
id SERIAL PRIMARY KEY,
city varchar ,
male_population bigint,
female_population bigint,
total_population bigint,
no_of_veterans bigint,
foreign_born bigint,
avg_household_size float,
race varchar, 
count bigint
);
""")


# INSERT RECORDS

city_table_insert = ("""INSERT INTO city_info (city,state_code,states) \
                 VALUES (%s, %s, %s)
                 ON CONFLICT (city) 
                DO NOTHING
""")

demograph_table_insert = ("""INSERT INTO demograph(city,male_population,female_population,total_population,no_of_veterans,foreign_born,avg_household_size,race,count) \
                 VALUES ( %s, %s, %s, %s,%s, %s, %s, %s, %s)
               
""")



# QUERY LISTS

create_table_queries = [city_table_create, demograph_table_create]
drop_table_queries = [city_table_drop, demograph_table_drop]