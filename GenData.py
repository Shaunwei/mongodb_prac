#!/usr/bin/env python
from pymongo import MongoClient
from conf import *
import bson

def input_data(client, filename):
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    key = KEY_VAULE

    with open(filename, 'rb') as f:
        line_content = f.readline()
        line_number = 0
        while(line_content):
            entry = {
                key: line_content, 
                'line_number': line_number,
            }
            print(entry)
            try:
                collection.insert(entry)
            except bson.errors.InvalidStringData:
                pass
            
            line_number += 1
            line_content = f.readline()
    
def reset_data(client):
    db = client[DATABASE_NAME]
    db.drop_collection(COLLECTION_NAME)

if __name__ == '__main__':
    client = MongoClient(HOST, PORT)
    reset_data(client)
    print('======reset=data=done======')

    input_data(client, FILE_NAME)

    print('=======================')
    print('Input data Done')
    print('=======================')
            
