import scrapy
from stackscraper.items import StackscraperItem
from scrapy.loader import ItemLoader


class QuestionsSpider(scrapy.Spider):
    name = 'questions'
    allowed_domains = ['stackoverflow.com']
    start_urls = ['http://stackoverflow.com/questions']

    def parse(self, response):
        self.logger.info('demo: scrape stackoverflow questions...')        
        questions = response.xpath("//a[@class='s-link']")
        for question in questions:
            loader = ItemLoader(item=StackscraperItem(), selector=question)

            loader.add_xpath('text', 'text()')
            loader.add_xpath('link', '@href')
            loader.add_xpath('id', '@href')

            yield loader.load_item()
            
        next_page = response.xpath('//a[@rel="next"]/@href').get()

        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)