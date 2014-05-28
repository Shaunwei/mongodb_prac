#!/usr/bin/env python
from pymongo import MongoClient
from bson.code import Code
from conf import *

MAP_FUN = Code("function() {"
               "  var summary = this.bio;"
               "    if (summary) {"
               "      summary = summary.toLowerCase().split(' ');"
               "        for (var i = summary.length - 1; i >= 0; i--) {"
               "          if (summary[i])  {"
               "            emit(summary[i], 1);"
               "          }"
               "        }"
               "    }"
               "};")

REDUCE_FUN = Code("function (key, values) {"
              "  var total = 0;"
              "  for (var i = 0; i < values.length; i++) {"
              "    total += values[i];"
              "  }"
              "  return total;"
              "}")

def map_reduce_word_count(client, map_fun, reduce_fun):
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    result = collection.map_reduce(map_fun, reduce_fun, KEY_VAULE)
    with open('temp', 'wb') as f:
        for r in result.find():
            tmp = r['_id'] + ', '+ str(r['value']) + '\n'
            f.write(tmp)
            print(tmp)

if __name__ == '__main__':
    client = MongoClient(HOST, PORT)
    map_reduce_word_count(client, MAP_FUN, REDUCE_FUN)
