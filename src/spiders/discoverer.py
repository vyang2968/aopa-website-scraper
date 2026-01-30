from datetime import datetime
from requests import Response
import scrapy
from scrapy_playwright.page import PageMethod


class DiscovererScrapy(scrapy.Spider):
    name = "discoverer"

    def start_requests(self):
        self.page_num = 0
        yield scrapy.Request(
            url="https://www.aopa.org/training-and-safety/learn-to-fly/flight-schools#t=tagFlightSchools&sort=%40aopaufbusinessname%20ascending&numberOfResults=100",
            meta={"playwright": True},
            callback=self.parse
        )

    def parse(self, response: Response):
        results = response.css('div.CoveoResult')
        self.logger.info(f"Found {len(results)} for page_num {self.page_num}")

        for result in results:
            link = result.css('a.CoveoResultLink::attr(href)').get()
            airport = result.css(
                'span[data-field="@aopaufairportident"] span:not(.coveo-field-caption)::text').get()

            if link:
                yield {
                    'source_url': link,
                    'airport': airport.strip(),
                    'page_num': self.page_num,
                    'queued_at': datetime.now().timestamp(),
                    'type': 'queue_item'
                }

        self.logger.info(
            "Done parsing all results for the page, moving onto the next one")
        self.page_num += 1

        if response.css('li.coveo-pager-next').get():
            yield scrapy.Request(
                url=response.url,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("click", "li.coveo-pager-next"),
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_selector", "a.CoveoResultLink"),
                    ],
                },
                callback=self.parse_aopa_flight_schools,
                dont_filter=True
            )

        if self.page_num > 30:
            self.logger.warning("Reached safety page limit. Stopping.")
            return
