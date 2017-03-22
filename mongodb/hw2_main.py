# Caution :  Do not add any print out statement.
from pymongo import MongoClient # Do pip install for using this lib.
import subprocess

import mongodb_query
from  user_definition import *

#Create connection 
client = MongoClient() #default-localhost:27017
#Connect to database
db = mongodb_query.database(client, dbname)


#Drop table.
mongodb_query.drop_table_query(db, collection_name)

#Insert all data from the input_file_name.
#(To be easy, let's load the entire .json file..)
#Q1.

mongoimport_query = mongodb_query.import_query(dbname, collection_name, input_file_name)
subprocess.call(mongoimport_query,shell=True)


# Q2.
#Add the field qty being the last two digits of itemId and add it into the document.
#COMPLETE THIS. HARDCODING THE FIELD NAMES IS ALLOWED.

document = db[collection_name].find_one()
items = document['items']

for item_dict in items:
    item_id = item_dict['itemId']
    qty = item_id % 100
    item_dict['qty'] = qty

db[collection_name].update({}, {"$set": {"items": items}})


# Q3. 
#Find First MLB Women's San Francisco Giants Short Sleeve Tops.
cursor = db[collection_name].find({"items.name":"MLB Women's San Francisco Giants Short Sleeve Top"}, {"items.$":1})
print cursor.next()



