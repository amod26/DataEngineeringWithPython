# ETL


# Data Pipeline.py
- connects to the .internal.db
- extracts the log file into a dataframe.
- splits the results column into received and total.
- loads the merged dataframes into a new table "New_data" from where the Analysts can query reports.

# Analyst Report.sql
- Gives the aggregated view of the database.

# Analyst Report Detailed.sql
- gives the detailed aggregated view of the database.

# Analyst Report.jpg
- Snapshot of the Analyst Report Detailed query.

# Solution Explanation.txt
- Part 2 explanation for the solution.

# Requirements.txt
- Contains the python packages necessary for the scripts to run.

# Bin/generate.py 
- Establishes the connection with Sqlite3.
- Creates a table if not exists.
- Generates random values in the table.

# Bin/reset.py
- Truncates the table using 'Delete' statement. (In Sqlite3, a delete statement without the where clause truncates the table)

# Data/students.db
- Contains the table(s) generated by the generate.py script.

# Data/API.py
- Starts the local server for API endpoint.

# Data/Test_log_generator.py
- gets a response from API and stores the data into an JSON file inside the Data folder.


