import scrapy
import re


class QuestionsSpider(scrapy.Spider):
    name = 'questions'
    allowed_domains = ['stackoverflow.com']
    start_urls = ['http://stackoverflow.com/questions']

    def parse(self, response):
        self.logger.info('demo: scrape stackoverflow questions...')        
        questions = response.xpath("//a[@class='s-link']")
        for question in questions:
            yield {
                'text': question.xpath('text()').get(),
                'link': question.xpath('@href').get(),
                'id': question.xpath('@href').get()[11:19]
            }

        next_page = response.xpath('//a[@rel="next"]/@href').get()

        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)