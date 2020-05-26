import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_log_file(cur,conn,cur2):
    # 
    """
    Reads the file and load it into an Dataframe. 
    """
    data = pd.read_csv('Data/us-cities-demographics.csv',sep=';')
    data.head(10)


    # insert city record
    """
    creates dataframe for city and drops duplicate
    """
        
    city_info=data[['City','State Code','State']]
    city_info.rename(columns={'State Code':'State_Code'}, 
                 inplace=True)
    city_info = city_info.drop_duplicates()

    
    for i, row in city_info.iterrows():
        city1 = (row.City,row.State_Code,row.State)#.encode('utf-8') 
#         city1 = u' '.join((row.City,row.State_Code,row.State)).encode('utf-8') .strip()

        try:
            cur.execute(city_table_insert,city1)
        except:
           # print(city1)
            continue 
    
    conn.commit()
    cur2 = conn.cursor()


    

    
    
     # insert demograph record
    """
    Creates the demograph dataframe 
    """
    demograph = data[['City','Male Population','Female Population','Total Population','Number of Veterans','Foreign-born','Average Household Size','Race','Count']]
    #print(demograph)
    demograph.rename(columns={'Male Population':'Male_Population','Female Population':'Female_Population','Total Population':'Total_Population','Number of Veterans':'Number_of_Veterans','Foreign-born':'Foreign_born','Average Household Size':'Average_Household_Size'}, 
                 inplace=True)
    
    demograph = demograph.fillna(0)
    
    for i, row in demograph.iterrows():
        demograph1 = (row.City,row.Male_Population,row.Female_Population,row.Total_Population,row.Number_of_Veterans,row.Foreign_born,row.Average_Household_Size,row.Race,row.Count)
        try:
            cur2.execute(demograph_table_insert, demograph1)
        except Exception as e:
            continue
    conn.commit()




def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    cur = conn.cursor()
    cur2 = conn.cursor()
    process_log_file(cur,conn,cur2)
    conn.close()
    


if __name__ == "__main__":
    main()