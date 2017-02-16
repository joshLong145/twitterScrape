import tweepy
import pymongo
import daemon
import redis
import time, datetime

class weather_data():
    
    def __init__(self,username):
        #Variables that contains the user credentials to access Twitter API 
        access_token = "813847838635524096-n5bz9BUEU3Q01ZDOgl0m2lVMEXiRgG7"
        access_token_secret = "5owS6OnDP0CI3gO2NbuZwepBfkk6cjRyjhfpJU3A27v9E"
        consumer_key = "Z2aWwTvk1hNyJyuVqeCPcGeBT"
        consumer_secret = "ADrWtAEKMkhOrajf9ETY60aW58O0ur53RDmBEZB6ivgOMdo5AC"
        # two levels of authentication for the twitter app
        auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        # creation of a tweepy obj
        api = tweepy.API(auth)
        # scrape a users timeline for the top 20 tweets, will include retweets
        stuff = api.user_timeline(screen_name = username, count = 100, include_rts = True)
        #array for storing tweet
        self.tweets = []
        # add url and text from tweet to tuple
        for status in stuff:
            #check for non ascii characters within text field
            text = ""
            for char in status.text:
                if ord(char) < 128:
                    text = text + char 
            # search for desired keywords in tweet 
            keywords = ["power outages","snow","storm","power","snow storm", "natural gas", "energy"]
            if any(x in text for x in keywords):
                data = ("https://twitter.com/statuses/" + str(status.id),text, status.created_at)
                # append tuple to list
                self.tweets.append(data) 
            
    # store all of a users tweets into an array
    def getData(self):
        return self.tweets

def getData():

    while True:
        user_names = ['UtilityDive', 'wunderground', 'UtilityDive','MidAm_EnergyCo','NEMA_web','SMUDUpdates','PSEGNews','JCP_L','SPPorg', 'PGE4Me', 'energizect' ] 
        # store each obj for the usernames in this list
        user_tweets = []
        for user in user_names:
            user_tweets.append( weather_data(user) )
        #setting up connect to DB
        mdb = pymongo.MongoClient().weather
        tweets = mdb.tweets
        # get each tuple from the list (if the list is empty, return)
        for user in user_tweets:
            arr = user.getData()
            for tweet in arr: 
                #json obj for storing data
                if(tweets.find_one( {"URL": tweet[0]} ) == None ):
                    data = {
                        "URL": tweet[0],
                        "text": tweet[1], 
                        "time": tweet[2]
                    }
                    #insert the tweet ino the collection
                    tweet_id = tweets.insert( data )
        #upload time stamp of each upload to redis 
        pool = redis.ConnectionPool(host = "127.0.0.1",port = 6379, db = 0)
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        my_server = redis.Redis(connection_pool = pool)
        my_server.set("time", st)
        time.sleep(100)

def run():
     with daemon.DaemonContext():
         getData()

if __name__ == "__main__":
    run()
