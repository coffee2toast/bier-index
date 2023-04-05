import datetime
import re
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
from multiprocessing.pool import ThreadPool

from typing import TYPE_CHECKING

from models import Beer, Review, User

from sqlalchemy.orm import Session
from models import Brewery
from typing import Optional

class BierIndexScraper:
    def __init__(self, session: Session):
        self._session = session

    def scrape_breweries(self):
        sitemap = requests.get("https://www.bier-index.de/sitemap.xml")
        brewery_urls = list(re.findall(r"<loc>(https://www.bier-index.de/brauereien/.+?\.html)<\/loc>", sitemap.text))
        for brewery_response in tqdm(ThreadPool().imap_unordered(requests.get, brewery_urls), total=len(brewery_urls), desc="Scraping Breweries"):
            self.try_parse_brewery(brewery_response)

    def scrape_beers(self):
        sitemap = requests.get("https://www.bier-index.de/sitemap.xml")
        beer_urls = list(re.findall(r"<loc>(https://www.bier-index.de/biere/.+?\.html)<\/loc>", sitemap.text))
        for beer_response in tqdm(ThreadPool().imap_unordered(requests.get, beer_urls), total=len(beer_urls), desc="Scraping Beers"):
            self.try_parse_beer(beer_response)

    def scrape_users(self):
        all_users = self._session.query(User).filter(User.id >= 0).all()
        all_user_urls = [f"https://www.bier-index.de/benutzer/{user.slug}.html" for user in all_users]
        for user_response in tqdm(ThreadPool().imap_unordered(requests.get, all_user_urls), total=len(all_user_urls), desc="Scraping Users"):
            self.try_parse_user(user_response)

    def try_parse_brewery(self, brewery_response: requests.Response):
        try:
            brewery_soup = BeautifulSoup(brewery_response.text, "html.parser")
            brewery_slug = brewery_response.url[len("https://www.bier-index.de/brauereien/"):-len(".html")]
            brewery_name = brewery_soup.find("h2").contents[0]
            brewery_id = brewery_soup.find("em", {"class": "id info"}).contents[0][1:]
            if self._session.query(Brewery.id).filter(Brewery.id == brewery_id).scalar() is not None:
                return
            self._session.add(Brewery(id=brewery_id, name=brewery_name, slug=brewery_slug))
        except Exception as e:
            print("Error while parsing brewery: ", brewery_response.url)
            from traceback import print_exc
            print_exc()

    def try_parse_beer(self, beer_response: requests.Response):
        try:
            beer_soup = BeautifulSoup(beer_response.text, "html.parser")
            beer_slug = beer_response.url[len("https://www.bier-index.de/biere/"):-len(".html")]
            beer_name = beer_soup.find("span", {"itemprop": "name"}).contents[0]
            beer_id = beer_soup.find("em", {"class": "id info"}).contents[0][1:]
            beer_alc_el = beer_soup.find("strong", {"class": "bier_alkohol"}).parent.contents[1]
            # beer_alc_el is either a form or a string
            if isinstance(beer_alc_el, str):
                beer_alc = float(beer_alc_el[:-len("% vol.")]) * 0.01
            else:
                beer_alc = None
            try:
                beer_brewery_slug = beer_soup.find("p", {"itemprop": "brand"}).find("a", {"class": "gray"})["href"][len("/brauereien/"):-len(".html")]
                beer_brewery_id = self._session.query(Brewery.id).filter(Brewery.slug == beer_brewery_slug).scalar()
            except AttributeError:
                beer_brewery_id = None
            beer = Beer(id=beer_id, name=beer_name, slug=beer_slug, alc_percentage=beer_alc, brewery_id=beer_brewery_id)
            if self._session.query(Beer.id).filter(Beer.id == beer_id).scalar() is not None:
                return
            self._session.add(beer)
            for review in beer_soup.findAll("section", {"itemprop": "review"}):
                self.try_parse_review(review, beer)
        except Exception as e:
            print("Error while parsing beer: ", beer_response.url)
            from traceback import print_exc
            print_exc()

    def try_parse_review(self, review_soup: BeautifulSoup, beer: Beer):
        try:
            review_id = int(review_soup.get("data-reviewid"))
            rating_element = review_soup.find("var", {"itemprop": "ratingValue"})
            if "index_none" in rating_element["class"]:
                rating = None
            else:
                rating = int(rating_element.contents[0][:-1])*0.01
            author_el = review_soup.find("span", {"itemprop": "author"})
            author_id = self.author_id_from_el(author_el)
            date_published = review_soup.find("span", {"itemprop": "datePublished"}).get("content")
            # parse YYYY-MM-DD to datetime
            date_published = datetime.datetime.strptime(date_published, "%Y-%m-%d")
            content = review_soup.find("p", {"itemprop": "reviewBody"}).get_text("\n")
            review = Review(id=review_id, percentage=rating, comment=content, date=date_published, user_id=author_id, beer=beer)
            if self._session.query(Review.id).filter(Review.id == review_id).scalar() is not None:
                return
            self._session.add(review)
        except Exception as e:
            print("Error while parsing review:", review_soup)
            from traceback import print_exc
            print_exc()
            return None

    def try_parse_user(self, user_response: requests.Response):
        try:
            user_soup = BeautifulSoup(user_response.text, "html.parser")
            user_slug = user_response.url[len("https://www.bier-index.de/benutzer/"):-len(".html")]
            user_description = user_soup.find("p", {"id": "user-desc"}).get_text("\n")
            user = self._session.query(User).filter(User.slug == user_slug).one()
            user.description = user_description
        except Exception as e:
            print("Error while parsing user: ", user_response.url)
            from traceback import print_exc
            print_exc()
        
    def author_id_from_el(self, author_soup: BeautifulSoup) -> int:
        author_link = author_soup.find("a")
        if author_link is None:
            uid = -2
            if self._session.query(User.id).filter(User.id == uid).scalar() is None:
                user = User(id=uid, name="Der Bierkeller")
                self._session.add(user)
            return uid
        if author_link.get("href") == '/die-redaktion/':
            uid = -1
            if self._session.query(User.id).filter(User.id == uid).scalar() is None:
                user = User(id=uid, name="Die Redaktion")
                self._session.add(user)
            return uid
        elif author_link.get("href").startswith('/benutzer/'):
            slug = author_link.get("href")[len('/benutzer/'):-1]
            uid = self._session.query(User.id).filter(User.slug == slug).scalar()
            if uid is None:
                user = User(slug=slug, name=author_link.contents[0])
                self._session.add(user)
                self._session.flush()
                uid = self._session.query(User.id).filter(User.slug == slug).scalar()
            return uid
        else:
            raise Exception("Unable to parse author: ", author_soup)
