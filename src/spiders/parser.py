from datetime import datetime
import sqlite3
from requests import Response
import scrapy
from scrapy_playwright.page import PageMethod


class ParserScrapy(scrapy.Spider):
    name = "parser"

    def start_requests(self):
        connection = sqlite3.connect("flight-schools.db")
        cursor = connection.cursor()

        cursor.execute(
            "SELECT url, airport, queued_at, page_num FROM queue WHERE status='pending'")

        for (url, airport, queued_at, page_num) in cursor.fetchall():
            yield scrapy.Request(
                url=url,
                meta={"playwright": True},
                callback=self.parse_details,
                cb_kwargs={
                    'meta': {
                        'airport': airport,
                        'queued_at': queued_at,
                        'page_num': page_num
                    }

                }
            )

        connection.close()

    def parse_details(self, response: Response, meta: dict[str, str]):
        name = response.css('h1[class*="header"]::text').get()
        airport = meta.get("airport")
        website = response.css('a[class*="website"]::text').get()
        aopa_source_url = response.url
        page_num = meta.get("page_num")
        time_to_scrape = (
            datetime.now() - datetime.formtimestamp(meta.get("queued_at"))).total_seconds()

        values = {
            'name': name,
            'airport': airport,
            'website': website,
            'aopa_source_url': aopa_source_url,
            'time_to_scrape': time_to_scrape,
            'page_num': page_num
        }

        self.logger.info(values)

        yield values
