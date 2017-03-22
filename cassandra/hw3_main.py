import json #json library
import time

import json_key_value
import cassandra_function
from  user_definition import *

#open a file ("input_file_name") as an input.
with open(input_file_name, 'r') as input_file:
    data = json.load(input_file)

#Connect to Cassandra
#https://datastax.github.io/python-driver/getting_started.html
#Instantiate a cluster
cluster = cassandra_function.Cluster()
session = cassandra_function.connect_session(cluster)

#Drop a keyspace
cassandra_function.drop_keyspace(session, keyspace)

# Q1
# Create a keyspace
cassandra_function.create_keyspace(session, keyspace)

# Choose a keyspace
session.set_keyspace(keyspace)

# Q2
# Create DB tables
table_name = "items"
column_and_type_list =  "itemId int, name varchar, shortDescription text, customerRating double, numReviews int, qty int"
primary_key_list =  "itemId"
cassandra_function.create_table(session, table_name,column_and_type_list, primary_key_list)

# Insert Data
column_names = "itemId, name, shortDescription, customerRating, numReviews, qty"

for item in json_key_value.get_data_value(data,'items'):
    itemId = json_key_value.get_data_value(item, 'itemId')
    name = json_key_value.get_data_value(item, 'name')
    shortDescription = json_key_value.get_data_value(item, 'shortDescription')
    customerRating = json_key_value.get_data_value(item, 'itemId')
    numReviews = json_key_value.get_data_value(item, 'numReviews')
    qty = itemId % 100

    name = name.replace("'", "''")
    shortDescription = shortDescription.replace("'", "''")

    values = "%s, '%s', '%s', %s, %s, %s" % (itemId, name, shortDescription, customerRating, numReviews, qty)

    cassandra_function.insert_into_table(session, table_name, column_names, values)

# FINISH THIS.
# TODO : WRITE CODE TO INSERT DATA FROM "data" in line 10. USE insert_into_table() FUNCTION IN cassandra_function.py
# Ex. cassandra_function.insert_into_table(session, "items", "itemId, name, ....", "VALUE, VALUE, .....")


# Q3
column_names = "*"
table_name = "items"
constraint = "name = 'MLB Women''s San Francisco Giants Short Sleeve Top'"
# cassandra_function.select_data(session,  table_name, column_names, constraint) # TRY IT AND COMMENT IT.

#Q4
question =  "Does select 'MLB Women''s San Francisco Giants Short Sleeve Top' work in the items table table work without ALLOW FILTERING?"
answer = "No" #Choose one
print ("%s - %s") %(question, answer)

# Q5
# Create Materized View
view_name = "materialized_items_view"
column_names = "*"
table_name = "items"
constraint = "name IS NOT NULL"
primary_keys = "name, itemId"
cassandra_function.create_materialized_view(session, view_name, column_names, table_name, constraint, primary_keys)

# Wait until materialized view is created.
select_table_query = "SELECT COUNT(*) AS ct FROM items"
base_table_row_count = session.execute(select_table_query)[0].ct
ct = 0
while ct != base_table_row_count:
    time.sleep(1)
    select_materialized_view_query = "SELECT COUNT(*) AS ct FROM materialized_items_view"
    rows = session.execute(select_materialized_view_query)
    ct= rows[0].ct
    
# Q6
# Select data from view
column_names = "*"
table_name =  "materialized_items_view"
constraint = "name = 'MLB Women''s San Francisco Giants Short Sleeve Top'"
returned_rows = cassandra_function.select_data(session,  table_name, column_names, constraint)
for each in returned_rows:
    print each

# Q7
table_name = "materialized_items_view"
column = "numReviews"
value = 10
constraint = "name = 'NFL Men''s San Francisco 49Ers C Hyde 28 Player Tee' and itemId = 52507967"
# cassandra_function.update_data(session, table_name, column, value, constraint)


question = "Can you update data in materialized views?"
answer = "No"
print ("%s - %s") %(question, answer)

# Q8
table_name = "items"
column = "numReviews"
value = 10
constraint = "itemId = 52507967"
cassandra_function.update_data(session, table_name, column, value, constraint)

column_names = "*"
table_name =  "items"
constraint = "itemId = 52507967"
returned_rows = cassandra_function.select_data(session,  table_name, column_names, constraint)
# for each in returned_rows:
#     print each

column_names = "*"
table_name =  "materialized_items_view"
constraint = "name = 'NFL Men''s San Francisco 49Ers C Hyde 28 Player Tee'"
returned_rows = cassandra_function.select_data(session,  table_name, column_names, constraint)
# for each in returned_rows:
#     print each


question = "When you update data in a base(original) table, is the content also updated in the corresponding materialized view?"
answer = "Yes"
print ("%s - %s") %(question, answer)

# Close communication.
cluster.shutdown()
