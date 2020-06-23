import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_events_table_truncate = "truncate table staging_events;"
customer_table_drop = "drop table if exists customer_dim;"
product_table_drop = "drop table if exists product_dim;"
transaction_table_drop = "drop table if exists transaction_fact;"


# CREATE TABLES

staging_events_table_create = ("""create table if not exists staging_events(
transaction_id varchar(500),
customer_name varchar(100),
member_id varchar(500),
product_id integer,
item varchar(500),
transaction_type varchar(100),
product_price varchar(500)
);
""")

customer_table_create = ("""create table if not exists customer_dim(
customer_id int identity(0,1) primary key,
member_id varchar(50),
customer_name varchar(100) not null
)
diststyle auto;
""")


product_table_create = ("""create table if not exists product_dim(
product_id bigint not null primary key,
item varchar(500) not null,
product_price varchar(20) not null
)
diststyle auto;
""")

transaction_table_create = ("""create table if not exists transaction_fact(
transaction_id varchar(50),
transaction_type varchar(20) not null,
product_id bigint not null,
customer_id bigint,
created_at datetime default sysdate sortkey
)
diststyle even;
""")


# STAGING TABLES

staging_events_copy = ("""copy staging_events(transaction_id ,customer_name ,member_id ,product_id ,item ,transaction_type,product_price )
from 's3://daily-transaction1'
iam_role 'arn:aws:iam::0123456789:role/AmodRedshiftRole'
IGNOREHEADER 1
delimiter ','
csv;
""")


# FINAL TABLES

customer_table_insert = (""" insert into customer_dim ( customer_name,member_id)
select Distinct s.customer_name
        ,s.member_id
from staging_events s left join customer_dim cd on s.customer_name = cd.customer_name;
""")


product_table_insert = (""" insert into product_dim(product_id, item, product_price)
select distinct
        product_id
        ,item
        ,ltrim(product_price,'$')::float as product_price
    from staging_events
    where product_id is not null;
""")


transaction_table_insert = (""" insert into transaction_fact(transaction_id, transaction_type,product_id,customer_id)
select e.transaction_id
        ,e.transaction_type
        ,e.product_id
        ,c.customer_id
    from staging_events e
    join customer_dim c on c.customer_name = e.customer_name
    where transaction_id is not null;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        customer_table_create, product_table_create, transaction_table_create]
drop_table_queries = [staging_events_table_drop,
                      customer_table_drop, product_table_drop, transaction_table_drop]
copy_table_queries = [staging_events_table_truncate, staging_events_copy]
insert_table_queries = [customer_table_insert,
                        product_table_insert, transaction_table_insert]
