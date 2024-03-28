import json
import pymongo


def extract_data(file):
    with open(file, 'r') as used_data:
        data = json.load(used_data)
    return data['users']


def transform_data(data):
    transformed_data = []
    for user in data:
        user_id = user['user_id']
        name = user['name']
        phone_number = user['phone_number']
        usage = user['usage']
        for usage_record in usage:
            transformed = {
                'User ID': user_id,
                'Name': name,
                'Phone Number': phone_number,
                'Date': usage_record['date'],
                'Minutes Used': usage_record['minutes_used'],
                'Data Used (GB)': usage_record['data_used_gb'],
                'Total Spent': usage_record['total_spent']
            }
            transformed_data.append(transformed)
    return transformed_data


client = pymongo.MongoClient("mongodb://localhost:27017")
db = client.used_data


def load_data(data, multiple=True):
    collection = db.users
    if multiple:
        return collection.insert_many(data).inserted_ids
    else:
        return collection.insert_one(data).inserted_id


def transform_data_2(data): #изменить на каждого пользователя
    total_minutes_used = 0
    total_data_used_gb = 0

    for row in data:
        total_minutes_used += row['Minutes Used']
        total_data_used_gb += row['Data Used (GB)']

    return total_minutes_used, total_data_used_gb


def load_data_2(data, multiple=True):   #доделать
    collection = db.total
    if multiple:
        return collection.insert_many(data).inserted_ids
    else:
        return collection.insert_one(data).inserted_id


data_file = 'used_data.json'
data = extract_data(data_file)
transformed_data = transform_data(data)
total_minutes, total_data_gb = transform_data_2(transformed_data)
print(transformed_data, total_data_gb, total_minutes)
load_data(transformed_data)

