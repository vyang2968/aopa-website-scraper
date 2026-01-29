from datetime import datetime
from typing import Any, Iterable
import scrapy
from scrapy.http import Response
from scrapy_playwright.page import PageMethod

class FlightSchoolSpider(scrapy.Spider):
    name = "flight-school-spider"

    def start_requests(self):
        self.page_num = 0

        yield scrapy.Request(
            url="https://www.aopa.org/training-and-safety/learn-to-fly/flight-schools#t=tagFlightSchools&sort=%40aopaufbusinessname%20ascending&numberOfResults=100",
            meta={"playwright": True, "playwright_page_methods": [
                PageMethod("wait_for_selector", "a.CoveoResultLink"),
            ], },
            callback=self.parse_aopa_flight_schools

        )

    def parse_aopa_flight_schools(self, response: Response):
        results = response.css('div.CoveoResult')

        self.logger.info(f"Found {len(results)} for page_num {self.page_num}")

        for result in results:
            start = datetime.now()

            link = result.css('a.CoveoResultLink::attr(href)').get()
            airport = result.css(
                'span[data-field="@aopaufairportident"] span:not(.coveo-field-caption)::text').get()

            if link:
                yield response.follow(
                    link,
                    callback=self.parse_flight_school_overviews,
                    cb_kwargs={
                        'source_url': link,
                        'meta': {
                            'airport': airport.strip(),
                            'start': start,
                            'page_num': self.page_num
                        }

                    },
                    meta={"playwright": True}
                )

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
                        PageMethod("wait_for_selector", "a.CoveoResultLink"),
                    ],
                },
                callback=self.parse_aopa_flight_schools,
                dont_filter=True
            )

    def parse_flight_school_overviews(self, response: Response, source_url: str, meta: dict[str, str]) -> Iterable[dict[str, Any]]:
        name = response.css('h1[class*="header"]::text').get()
        airport = meta.get("airport")
        website = response.css('a[class*="website"]::text').get()
        aopa_source_url = source_url
        time_to_scrape = (datetime.now() - meta.get("start")).total_seconds()
        page_num = meta.get("page_num")
        
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
