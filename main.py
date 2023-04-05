import re
from typing import List

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from models import Base
from scraper import BierIndexScraper

def main():
    engine = create_engine("sqlite:///bier-index.db", echo=False, pool_size=512, max_overflow=0)
    Base.metadata.create_all(engine)
    global Session
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)

    session = Session()
    scraper = BierIndexScraper(session)

    scraper.scrape_breweries()
    scraper.scrape_beers()
    scraper.scrape_users()

    session.commit()

if __name__ == "__main__":
    main()
