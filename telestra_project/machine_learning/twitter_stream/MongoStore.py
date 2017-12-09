import pymongo
from pymongo import MongoClient

#-------------- Connecting, creating data base
client = MongoClient('localhost', 27017)
DataBase = client['DataBase_1']
Collection_1 = DataBase['DataBase_1']

#-------------- Inserting data
import datetime
post = {"author": "Ahmed",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.datetime.utcnow()}
# The above post is for test, the tweets will be extracted in rendered as json.

posts = DataBase.posts
post_id = posts.insert(post)
print "Document ID is: %s", post_id
print 'Collection name is: %s ',DataBase.collection_names()
#ObjectId('...')
