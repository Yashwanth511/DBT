import pandas as pd
from kafka import KafkaProducer
import time
import json

KAFKA_TOPIC_1 = "positive"
KAFKA_TOPIC_2 = "negative"
KAFKA_TOPIC_3 = "neutral"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"


def send_three_topics(tweet_list):
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    for message in tweet_list:
        content = message["tweet"]
        kafka_tweet = {"msg": content, "count": content.count("#")}
        kafka_sentiment = str(message["sentiment"])

        if kafka_sentiment == "positive":
            kafka_producer.send(KAFKA_TOPIC_1, value=kafka_tweet)

        elif kafka_sentiment == "negative":
            kafka_producer.send(KAFKA_TOPIC_2, value=kafka_tweet)

        else:
            kafka_producer.send(KAFKA_TOPIC_3, value=kafka_tweet)

        print("Tweet: ", content)
        print("Sentiment: ", kafka_sentiment)
        time.sleep(5)
    kafka_producer.flush()
    kafka_producer.close()


if __name__ == "__main__":
    print("Kafka Producer has Started ... ")
    filepath = "dataset.xlsx"
    tweet_df = pd.read_excel(filepath, usecols=["tweet", "sentiment"], header=0)
    tweet_list = tweet_df.to_dict(orient="records")
    send_three_topics(tweet_list)
    print("Kafka Producer has Completed!. ")
