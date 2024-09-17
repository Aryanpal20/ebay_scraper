import json
import logging

BOT_NAME = 'ebay_scraper'
SPIDER_MODULES = ['ebay_scraper.spiders']
NEWSPIDER_MODULE = 'ebay_scraper.spiders'
ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS = 1000  
DOWNLOAD_DELAY = 0.01  
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 503, 504, 400, 429, 408]
LOG_FILE = 'scrapy.log'
LOG_LEVEL = 'INFO'

# Load settings from configuration file
with open('config.json') as f:
    config = json.load(f)

# Custom settings
CHUNK_SIZE = config.get('CHUNK_SIZE')
SLEEP_TIME = config.get('SLEEP_TIME')
INITIAL_RETRY_SLEEP = config.get('RETRY_TIMES')
PROXY_ENABLED = config.get('PROXY')
PROXY_LIST_FILE = config.get('PROXY_FILE')
OUTPUT_FORMAT = config.get('OUTPUT_FORMAT')

# real time Logging configuration
# Logging configuration
logging.basicConfig(
    level=logging.DEBUG,  # Capture all logs for the file
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',  # Customize format as per your requirement
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
    ]
)

# Create a custom console handler that only prints INFO level and above
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Filter out DEBUG messages from console handler
class InfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno in (logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL)

console.addFilter(InfoFilter())
