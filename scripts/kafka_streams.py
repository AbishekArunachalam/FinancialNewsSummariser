from functools import lru_cache
from typing import Any
import finnhub
#import websockets
import datetime
import yaml
from pyspark.sql import SparkSession, functions as F, dataframe
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient
import socket

class KakfaStream():

    def __init__(self):
        with open(f"../conf/config.yml", "r") as auth:
            auth_cfg = yaml.load(auth, Loader=yaml.Loader)
        with open("../conf/stocks.yml", "r") as st:
            stock_cfg = yaml.load(st, Loader=yaml.Loader)

        self.spark = SparkSession.builder.getOrCreate()

        self.finhub_api_key = auth_cfg['auth']['api_key']
        self.domains = stock_cfg['stocks'].keys()
        self.stock_list = [{domain: stock_cfg['stocks'][domain]} for domain in stock_cfg['stocks']]
        self.query_date = datetime.date.today()
   
        auth_cfg['producer']['client.id'] =  socket.gethostname()
        self.producer_conf = auth_cfg['producer']
        self.consumer_conf = auth_cfg['consumer']

        self.admin_client = AdminClient({"bootstrap.servers": "localhost:9093"})
        print(f"List of topics in broker: {self.admin_client.list_topics().topics}")

    @lru_cache
    def get_news_articles(self, idx: int, domain: str) -> dataframe:
        """
        Query API to get the news data by domain and stock

        Args:
        idx: index of the item in the list
        domain: domain to retrieve the list of stocks

        Return:
        stocks_df: dataframe of stocks
        """
        finnhub_client = finnhub.Client(api_key= self.finhub_api_key)

        responses = list()
        try:
            for stock in self.stock_list[idx][domain]:
                response = finnhub_client.company_news(stock, 
                                                       _from= self.query_date - datetime.timedelta(days=1), 
                                                       to= self.query_date)
                responses.append(response)

            stocks_df = self.spark.createDataFrame(data=responses[0], 
                                                   schema = list(response[0].keys()))
            stocks_df.show()
            stocks_df = stocks_df.select('related','datetime','headline','summary') \
                    .withColumnRenamed('related','company') \
                    .orderBy(F.col('datetime'))
        except ValueError:
            print(f"Response object for domain {domain} is empty")
            stocks_df = None
        except IndexError:
            print(f"Response object for domain {domain} is empty")
            stocks_df = None
        except KeyError:
            print("Domain not registered in the stocks config file") 
            stocks_df = None

        finally:
            pass

        return stocks_df

    def msg_process(self, msg) -> None:
        """
        Convert message in bytes to UTF-8 

        Args:
        msg: message from Kakfa producer as bytecode
        """
        print(f"Message received: {msg.value().decode('utf-8')}")

    def kafka_produce_news(self) -> list:
        """
        Ingest data to kafka topics using producers

        Returns:
        valid_domains: list of domains with data
        """
        domains = list(self.domains)
        valid_domains = list()

        for idx, domain in enumerate(domains):
            df = self.get_news_articles(idx, domain)

            if df:
                producer = Producer(**self.producer_conf)
                valid_domains.append(domain)
                try:
                    for row in df.collect():
                        producer.produce(topic = domain, value = str(row.asDict())) #, callback= acked)
                except:
                    print("There was some error")

                finally:
                    producer.flush()
                print(f"{domain.upper()} news published to topics")
            else:
                pass

        return valid_domains

    def kafka_consume_news(self, valid_domains: list) -> None:
        """
        Kafka consumer to receive the news feed

        Args:
        valid_domains: list of domains with data
        """
        if len(valid_domains) > 0:
            print(f"Domains with data buffered: {valid_domains}")

            consumer = Consumer(self.consumer_conf)
            consumer.subscribe(valid_domains)

            msg = consumer.poll(timeout=5)
            self.msg_process(msg)
            consumer.commit(msg)

            # Close down consumer to commit final offsets.
            consumer.close()

            print("News consumed from topics")
        else:
            pass

class KafkaUtils():

    def __init__(self):
        self.admin_client = AdminClient({"bootstrap.servers": "localhost:9093"})

    def delete_topic(self, topic_list) -> None:
        """
        Delete a topic from the Kafka broker
        Args:
        topic_list: list of topics to delete
        """
        self.admin_client.delete_topics(topics= topic_list)
        print(f"The topics {topic_list} have been deleted from the broker")
    
    def acked(err, msg):
        """
        
        """
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

if __name__ == '__main__':
    obj = KakfaStream()
    valid_domains = obj.kafka_produce_news()
    obj.kafka_consume_news(valid_domains)

"""
from kafka import KafkaConsumer
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:1234')
print(producer)
finnhub

df = spark.createDataFrame(data=dataDictionary, schema = ["name","properties"])
df.printSchema()
df.show(truncate=False)

Alpaca websocket

async def handler():
    async with websockets.connect('wss://api.alpaca.markets/stream') as ws:
        {"action": "auth", "key": "", "secret": ""}
        {"action":"subscribe","news":["*"]}

        # subscribe
        auth = {"action": "auth", "key": "", "secret": ""}
        await ws.send(json.dumps(auth))

        data = {"action":"subscribe","news":["*"]}
        await ws.send(json.dumps(data))

        # get all messages (not only with `update`)
        async for message in ws:
            print(message)

"""