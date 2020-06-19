# Moving files from S3 to AWS Redshift.

Parsing files on AWS S3, modeling, staging in Redshift and loading in Redshift table.

### Things to take care of:
- Every 5 months, data doubles. So to sustain the ever increasing load of data, we need to create a schema that supports both scalability and efficiency in retrieving the data.
