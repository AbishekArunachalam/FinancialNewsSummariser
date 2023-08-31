#!/usr/bin/env python3

import finnhub
import yaml
import sys
import unittest
from unittest.mock import patch
import datetime
from pyspark.sql import SparkSession, functions as F, dataframe


class TestKakfaStream(unittest.TestCase):

    def setUp(self):
        with open(f"../conf/config.yml", "r") as auth:
            auth_cfg = yaml.load(auth, Loader=yaml.Loader)
        with open("../conf/stocks.yml", "r") as st:
            stock_cfg = yaml.load(st, Loader=yaml.Loader)
        self.spark = SparkSession.builder.getOrCreate()
        self.finhub_api_key = auth_cfg['auth']['api_key']
        self.response_data = {'company': 'JNJ', 'datetime': 1693291860, 'headline': 'Biden unveils first 10 drugs subject to Medicare price negotiation', 'summary': 'Looking for stock market analysis and research with proves results? Zacks.com offers in-depth financial research with over 30years of proven results.'}
        self.query_date = datetime.date.today()
        self.stock_list = [{domain: stock_cfg['stocks'][domain]} for domain in stock_cfg['stocks']]

    @patch('finnhub.Client')
    def test_get_news_articles_assert_value(self, mock_get) -> None:
        """
        Mock and test response from API
        """
        mock_get.return_value = self.response_data
        val = KakfaStream.get_news_articles(self, idx= 1, domain = "NDSDDS")
        self.assertEqual(val, None)
    
    def test_get_news_articles_assert_type(self) -> None:
        """
        Mock and test response from API
        """
        val = KakfaStream.get_news_articles(self, idx= 0, domain = "tech")
        self.assertIsInstance(list(val), list)
        
if __name__ == '__main__':
    sys.path.append('/Users/abishekarunachalam/GenerativeAI/FinancialNews/git/FinancialNewsSummariser')
    from scripts.kafka_streams import KakfaStream
    unittest.main()