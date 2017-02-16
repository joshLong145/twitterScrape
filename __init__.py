# using flask to serve python to apache 
import os
from flask import Flask, render_template, request
import pymongo
#flask object 
app = Flask(__name__)
#routing for flask
@app.route("/")
def index(): 
    twitter_data = []
    mdb = pymongo.MongoClient().weather
    tweets = mdb.tweets
    for tweet in tweets.find().sort("time",pymongo.DESCENDING):
        data = (tweet['URL'],tweet['text'],tweet['time'],tweet['_id'])
        twitter_data.append(data)
    return render_template("index.html", tweets = twitter_data)
