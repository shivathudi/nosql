import json

import json_key_value
import postgres_function
from  user_definition import *

with open(input_file_name, 'r') as input_file:
    data = json.load(input_file)
    #Q1
    print(json_key_value.get_data_value(data, "totalResults"))
    #Q2
    print(json_key_value.count_data(data, "items"))

#open database
#given as a parameter
db_conn = postgres_function.connectdb(dbname, usr_name)
cursor = postgres_function.db_cursor(db_conn)

#Q3
# Drop tables if they exist, else continue

# Create DB tables, inventory and items
table_name = "inventory"
column_and_type_list = "itemId INTEGER, qty INTEGER"

# Check if table exists, and drop it if it does
cursor .execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
if cursor.fetchone()[0]:
    postgres_function.drop_table(cursor,table_name)

postgres_function.create_table(cursor, table_name, column_and_type_list)

table_name = "items"
column_and_type_list = "itemId INTEGER, name VARCHAR, shortDescription VARCHAR, customerRating REAL, numReviews INTEGER"

# Check if table exists, and drop it if it does
cursor .execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
if cursor.fetchone()[0]:
    postgres_function.drop_table(cursor,table_name)

postgres_function.create_table(cursor, table_name, column_and_type_list)

# FINISH THIS.
# TODO : WRITE CODE TO INSERT DATA FROM "data" in line 9. USE insert_into_table() FUNCTION IN postgres_function.py

table_name = "inventory"
column_names = "itemId, qty"

for item_dict in json_key_value.get_data_value(data, 'items'):
    item_id = json_key_value.get_data_value(item_dict, 'itemId')
    qty = int(str(item_id)[-2:])
    values = "%s, %s" % (item_id, qty)
    postgres_function.insert_into_table(cursor, table_name, column_names, values)

table_name = "items"
column_names = "itemId, name, shortDescription, customerRating, numReviews"

for item_dict in json_key_value.get_data_value(data, 'items'):
    item_id = json_key_value.get_data_value(item_dict, 'itemId')
    name = json_key_value.get_data_value(item_dict, 'name')
    name = name.replace("'", "''")
    try:
        shortDescription = json_key_value.get_data_value(item_dict, 'shortDescription')
        shortDescription = shortDescription.replace("'", "''")
    except:
        shortDescription = "NULL"
    try:
        customerRating = json_key_value.get_data_value(item_dict, 'customerRating')
    except:
        customerRating = "NULL"
    try:
        numReviews = json_key_value.get_data_value(item_dict, 'numReviews')
    except:
        numReviews = "NULL"

    if shortDescription == "NULL":
        values = "%s, '%s', %s, %s, %s" % (item_id, name, shortDescription, customerRating, numReviews)
    else:
        values = "%s, '%s', '%s', %s, %s" % (item_id, name, shortDescription, customerRating, numReviews)
    postgres_function.insert_into_table(cursor, table_name, column_names, values)

# Ex.  postgres_function.insert_into_table(cursor, "items", "itemId, name", "VALUE, VALUE")

# Q4
# Select Data
table_name = "inventory"
column_names = "*"
constraint = "itemId IN (SELECT itemId from items WHERE name = 'MLB Women''s San Francisco Giants Short Sleeve Top')"
postgres_function.select_data(cursor, table_name, column_names, constraint)
print cursor.fetchone()

db_conn.commit() #make the changes to the db persistent.

#close communication with database.
cursor.close()
db_conn.close()


