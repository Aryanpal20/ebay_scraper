import random
import logging

class ProxyMiddleware:
    def __init__(self, proxies):
        self.proxies = proxies
        self.failed_proxies = {}

    @classmethod
    def from_crawler(cls, crawler):
        proxy_file = crawler.settings.get('PROXY_LIST_FILE')
        with open(proxy_file, 'r') as f:
            proxies = [line.strip() for line in f]
        return cls(proxies)

    def process_request(self, request, spider):
        if self.proxies:
            proxy = random.choice(self.proxies)
            request.meta['proxy'] = proxy

    def process_response(self, request, response, spider):
        if response.status in spider.custom_settings['RETRY_HTTP_CODES']:
            proxy = request.meta.get('proxy')
            if proxy:
                self.failed_proxies[proxy] = self.failed_proxies.get(proxy, 0) + 1
                if self.failed_proxies[proxy] > spider.custom_settings['RETRY_TIMES']:
                    self.proxies.remove(proxy)
                    spider.logger.info(f"Proxy {proxy} excluded due to failure")
            return self._retry(request)
        return response

    def process_exception(self, request, exception, spider):
        proxy = request.meta.get('proxy')
        if proxy:
            self.failed_proxies[proxy] = self.failed_proxies.get(proxy, 0) + 1
            if self.failed_proxies[proxy] > spider.custom_settings['RETRY_TIMES']:
                self.proxies.remove(proxy)
                spider.logger.info(f"Proxy {proxy} excluded due to failure")
        return self._retry(request)

    def _retry(self, request):
        retryreq = request.copy()
        retryreq.dont_filter = True
        retryreq.priority = request.priority + 1
        return retryreq
