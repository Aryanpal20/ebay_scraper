import scrapy
import json
import logging
from scrapy.utils.project import get_project_settings
from ebay_scraper.items import EbayScraperItem
import asyncio

class EbaySpider(scrapy.Spider):
    name = 'ebay'
    custom_settings = {
        'RETRY_TIMES': get_project_settings().get('RETRY_TIMES'),
        'RETRY_HTTP_CODES': get_project_settings().get('RETRY_HTTP_CODES'),
        'DOWNLOADER_MIDDLEWARES': {
            'ebay_scraper.middlewares.ProxyMiddleware': 543,
        },
        'ITEM_PIPELINES': {
            'ebay_scraper.pipelines.SavingPipeline': 300,
        },
        'CONCURRENT_REQUESTS': 1000,  
        'DOWNLOAD_DELAY': 0.01, 
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1000,
        'CONCURRENT_REQUESTS_PER_IP': 1000,
    }

    def start_requests(self):
        with open('input.json') as f:
            data = json.load(f)

        chunk_size = get_project_settings().get('CHUNK_SIZE', 300)
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            for keyword_info in chunk:
                keyword = keyword_info["keyword"]
                links = keyword_info["links"]
                for url in links:
                    yield scrapy.Request(url, self.parse_response)

    def parse_response(self, response):
        item = EbayScraperItem()
        item['url'] = response.url
        item['status'] = response.status
        item['content'] = response.text
        yield item