# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


import datetime
import sqlite3
from datetime import datetime

import scrapy


class DatabasePipeline:
    def open_spider(self, spider: scrapy.Spider):
        self.connection = sqlite3.connect("flight-schools.db")
        self.cursor = self.connection.cursor()

        self.start_time = datetime.now()

        self.cursor.execute(
            """
          CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT UNIQUE,
            airport TEXT,
            page_num INTEGER,
            queued_at INTEGER,
            "type" TEXT,
            website TEXT,
            error TEXT
          );
          """
        )
        self.connection.commit()

        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS schools(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            airport TEXT,
            website TEXT,
            aopa_source_url TEXT UNIQUE,
            time_to_scrape INTEGER,
            page_num INTEGER
        );
          """
        )
        self.connection.commit()

    def process_item(self, item: dict[str, str], spider: scrapy.Spider):
        name = item.get("name")
        airport = item.get("airport")
        website = item.get("website")
        aopa_source_url = item.get("aopa_source_url")
        time_to_scrape = item.get("time_to_scrape")
        page_num = item.get("page_num")
        error = item.get("error")

        clean_name = name if isinstance(name, str) else None
        clean_airport = airport if isinstance(airport, str) else None

        try:
            self.cursor.execute(
                """
            INSERT OR IGNORE INTO schools (name, airport, website, aopa_source_url, time_to_scrape, page_num, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    clean_name,
                    clean_airport,
                    website,
                    aopa_source_url,
                    time_to_scrape,
                    page_num,
                    error
                )
            )
            self.connection.commit()
        except Exception as e:
            spider.logger.error(e)

        return item

    def close_spider(self):
        duration = (datetime.now() - self.start_time).total_seconds()

        count = self.cursor.execute(
            """
        SELECT count(id)
        FROM schools
        """
        )
        self.connection.commit()

        print(f'Took {duration} time to scrape {count} schools')

        self.connection.close()
