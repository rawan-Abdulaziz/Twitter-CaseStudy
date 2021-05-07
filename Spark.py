import findspark
findspark.init()
import tweepy as tw
import pandas as pd
import jsonpickle
from json import loads
from textblob import TextBlob
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import DoubleType, StructField, StructType, IntegerType, DateType, StringType, LongType
from pyspark.streaming.kafka import KafkaUtils


def foreachrdd (rdd, spark):
    if not rdd.isEmpty():
        rdd = rdd.map(loadanddecode)
        schema = StructType([
            StructField("User_ID", StringType()),
            StructField("User_Name", StringType()),
            StructField("User_Screen_Name", StringType()),
            StructField("Followers_Count", IntegerType()),
            StructField("Friends_Count", IntegerType()),
            StructField("Verified", StringType()),
            StructField("Tweet_ID", StringType()),
            StructField("Tweet_Text", StringType()),
            StructField("Tweet_Date", DateType()),
            StructField("Source", StringType()),
            StructField("Retweet_Count", IntegerType()),
            StructField("Likes", IntegerType()),
            StructField("Result", StringType())
        ])
        df = spark.createDataFrame(rdd,schema)
        df.show()
        df.write.mode("append").parquet("hdfs://sandbox-hdp.hortonworks.com:8020//apps//hive//warehouse//casestudy.db//realdata//")
        


def loadanddecode (x):
    data = x[1]
    dataStr = loads(data).encode('utf-8')
    dataObj = jsonpickle.decode(dataStr)
    rep = get_tweet_sentiment_reply(dataObj)
    result = rep.split(',')[0]
    reply = rep.split(',')[1]
    dataObj.update({'Result': result})
    dataObj.update({'Reply': reply})
    User_ID = dataObj['User_ID']
    User_Name = dataObj['User_Name']
    User_Screen_Name = dataObj['User_Screen_Name']
    Followers_Count = dataObj['Followers_Count']
    Friends_Count = dataObj['Friends_Count']
    Verified = dataObj['Verified']
    Tweet_ID = dataObj['Tweet_ID']
    Tweet_Text = dataObj['Tweet_Text']
    Tweet_Date = dataObj['Tweet_Date']
    Source=dataObj['Source']
    Retweet_Count=dataObj['Retweet_Count']
    Likes=dataObj['Likes']
    Tweet_Result=dataObj['Result']
    row=[User_ID,User_Name,User_Screen_Name,Followers_Count,Friends_Count,Verified,Tweet_ID,Tweet_Text,Tweet_Date,Source,Retweet_Count,Likes,Tweet_Result]
    return row





def get_tweet_sentiment_reply(tweets):
    tweet = tweets["Tweet_Text"]
    user = tweets["User_Name"]
    id = tweets["Tweet_ID"]
    analysis = TextBlob(tweet)
    if analysis.sentiment.polarity > 0:
        result = "positive"
        reply_tweet = 'Thank You For Your Support'
    elif analysis.sentiment.polarity == 0:
        result = "neutral"
        reply_tweet = 'Thank You !!'
    else:
        result = "negative"
        reply_tweet = 'How Can I Help You ?'
    try:
       api.update_status(status=reply_tweet, in_reply_to_status_id=id, auto_populate_reply_metadata=True)
    except:
         print("Already Replied")
    return result+','+reply_tweet





consumer_key = '######################'
consumer_secret = '##############################################'
access_token = '#################################################'
access_token_secret = '############################################'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

warehouseLocation="hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse"


# context
sc = SparkContext()

# To avoid unncessary logs
sc.setLogLevel("WARN")
spark = SparkSession.builder.config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
#sc = spark.sparkContext
#spark=SparkSession(sc).enableHiveSupport()

# batch duration, here i process for each second
ssc = StreamingContext(sc, 5)


kafkaStream = KafkaUtils.createDirectStream(ssc, topics=['kafka topic name'], kafkaParams={"metadata.broker.list": "sandbox-hdp.hortonworks.com:6667"})



kafkaStream.foreachRDD(lambda rdd: foreachrdd(rdd, spark))


ssc.start()
ssc.awaitTermination()


