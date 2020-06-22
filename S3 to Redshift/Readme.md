Readme
==================

## Objective:
- To create an ETL pipeline to parse daily uploaded transaction csv file(s) in S3 and load it inside Redshift Data warehouse.


## Implementation:

- We will use the copy command of Redshift to copy the csv file from S3 bucket into the Redshift staging table and then to the schema tables.
- AWS services used are S3, Cloud9, Redshift.


## Schema Design:

### Fact table
1. transaction_fact: Contains details of transactions along with timestamp.  <br>
    - transaction_id, transaction_type, product_id, customer_id, created_at

### Dimension table
1. product_dim: Contains product details.<br>
    - product_id, item, product_price

2. customer_dim: Contains customer details. <br>
    - customer_id, customer_name, member_id


## Steps to run the script:
1. Enter your AWS Redshift credentials inside the dwh.cfg file.
2. In the sql_queries.py, add your bucket name and iam_role in copy command.
3. Just for the first time, run the create_tables.py.
4. run etl.py

## Contents of file:
1. dwh.cfg
2. create_tables.py
3. etl.py
4. sql_queries.py
5. requirement.txt


