import scrapy

class EbayScraperItem(scrapy.Item):
    url = scrapy.Field()
    status = scrapy.Field()
    content = scrapy.Field()


