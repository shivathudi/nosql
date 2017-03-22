import json

def count_data(data, key):
    return len(data[key])

def get_data_value(data,key):
    return data[key]

input_file_name = 'walmart_search_san_francisco.json'

with open(input_file_name, 'r') as input_file:
    data = json.load(input_file)
    #Q1
    # print(get_data_value(data, "totalResults"))
    #Q2
    # print(count_data(data, "items"))

my_tuples =[]

# for item_dict in get_data_value(data, 'items'):
#     item_id = get_data_value(item_dict, 'itemId')
#     name = get_data_value(item_dict, 'name')
#     try:
#         shortDescription = get_data_value(item_dict, 'shortDescription')
#     except:
#         shortDescription = "NULL"
#     try:
#         customerRating = get_data_value(item_dict, 'customerRating')
#     except:
#         customerRating = "NULL"
#     try:
#         numReviews = get_data_value(item_dict, 'numReviews')
#     except:
#         numReviews = "NULL"
#
#     values = "%s, %s, %s, %s, %s" % (item_id, name, shortDescription, customerRating, numReviews)
#     my_tuples.append((item_id, name, shortDescription, customerRating, numReviews))
#
#
# print my_tuples[:15]
# print len(my_tuples)


s = "hello's there"
a = s.replace("'","''")

print repr(a)