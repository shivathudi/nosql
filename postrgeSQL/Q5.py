import json
from json_key_value import *
from user_definition import *

with open(input_file_name, 'r') as input_file:
    data = json.load(input_file)

items_seen = ["itemId",  "qty", "name", "shortDescription", "customerRating", "numReviews"]

unused_fields = set()
counter_dict = {}

for item_dict in get_data_value(data, "items"):
    for key in item_dict.keys():
        if key not in items_seen:
            unused_fields.add(key)
            if key not in counter_dict:
                counter_dict[key] = 1
            else:
                counter_dict[key] += 1


for each in sorted(counter_dict, key=counter_dict.get):
    print "%s, %s" % (each, counter_dict[each])





