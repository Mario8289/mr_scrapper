import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import datetime as dt
import pandas as pd
import logging

# from ...scrap import Scrap
from mr_scrapper.scrap import Scrap


class Property:
    def __init__(self,
                 search_date: str,
                 listed_date: str,
                 listing_type: str,
                 marketed_by: str,
                 address: str,
                 postcode: str,
                 postcode_district: str,
                 price: int,
                 bathrooms: int,
                 bedrooms: int,
                 reception: int,
                 image: str,
                 description: str):
        self.search_date: search_date
        self.listed_date: listed_date
        self.listing_type: listing_type
        self.marketed_by: marketed_by
        self.address: address
        self.postcode: postcode
        self.postcode_district: postcode_district
        self._price: price
        self.bathrooms: bathrooms
        self.bedrooms: bedrooms
        self.reception: reception
        self.image: image
        self.description: description

    @property
    def price(self):
        price = self._price
        return price

    @price.setter
    def price(self, value):
        try:
            return int(value.split(' ')[0][1:].replace(",", ""))
        except Exception as e:
            raise e

zoopla_class_mapping = {
    "propertyPrice": "css-18tfumg-Text eczcs4p0",
    "listedDate": "css-19cu4sz-Text eczcs4p0",
    "propertyImage": "e2uk8e3 css-qbjov2-StyledLink-Link-ImageLink e33dvwd0",
    "address": "css-wfe1rf-Text eczcs4p0"
}


class ScrapZoopla(Scrap):
    def __init__(self):
        self.logger = logging.getLogger("ScrapZoopla")
        self.results: list = []

    def run(self, post_code_district: str,
            listing_type: str = "for-sale",
            search_radius: int = 0,
            mode: str = 'w',
            max_page=None,
            sleep_between_pages=1):

        page: int = 1
        last_page: bool = False

        try:

            while not last_page:
                url = f"https://www.zoopla.co.uk/{listing_type}/property/{post_code_district}/" \
                      f"?identifier={post_code_district}&q={post_code_district}&search_source=" \
                      f"refine&radius={search_radius}&pn="
                url += str(page)
                res = self.fetch(url)

                content = BeautifulSoup(res.text, 'lxml')

                if self.content_empty(content) or self.no_results_found(content):
                    last_page = True
                else:
                    last_page = self.parse(content, listing_type, post_code_district)

                    ## if you want to limit your search to n pages
                    if max_page:
                        if page > max_page:
                            break

                    page += 1

                    ## sleep duration between pages in order to alisite api requrest breach attention
                    time.sleep(sleep_between_pages)

            if self.results.__len__() != 0:
                return self.results_to_string()
            else:
                return None

        except Exception as e:
            print(f"you mess'd up bra, postcode district is not playing nice {post_code_district}")
            raise e

    @staticmethod
    def content_empty(content):
        obj = content.find("div", {'class': 'layout-standard'})
        if obj:
            return "Sorry" in obj.text.split('\n')[1]
        else:
            return False

    @staticmethod
    def no_results_found(content):
        obj = content.findAll("div", {"content": "No results found"})
        if obj:
            return obj[0].text == 'No results found'
        else:
            return False

    def fetch(self, url: str):
        print('HTTP GET request to URL: %s' % url, end='')
        res = requests.get(url)
        print(' | Status code: %s' % res.status_code)

        return res

    def get_text(self, card, tag=None, ref=None, name=None):
        if not any([tag is None, ref is None, name is None]):
            try:
                return card.find(tag, {ref: name}).text
            except:
                return None
        elif tag is not None and ref is None and name is None:
            try:
                return card.find(tag).text
            except:
                return None

    def get_int(self, value):
        try:
            return int(value)
        except ValueError:
            try:
                return int(value.split(' ')[0][1:].replace(",", ""))
            except ValueError:
                return None  # Type POA
        except TypeError:
            return None
        except Exception as e:
            raise e

    def parse(self, content, listing_type, post_code_district):

        cards = content.findAll('div', {'data-testid': 'search-result'})

        property_types = ['bungalow', 'detached house', 'semi-detached house', 'terraced house', 'town house',
                          'end terrace house', 'cottage', 'lodge', 'park home', 'house', 'property', 'flat',
                          'maisonette', 'studio', 'room', 'apartment']

        for card in cards:
            self.results.append({
                'search_date': dt.datetime.now().strftime('%Y-%m-%d'),
                'listed_date': pd.to_datetime(' '.join(
                    self.get_text(card, "span", "data-testid", "date-published").split(' ')[-3:])).strftime("%Y-%m-%d"),
                'listing_type': listing_type,
                'marketed_by': self.get_marketed_by(card),
                'type': [x for x in property_types if x in card.contents[0].contents[1].contents[2].text.lower()][0] if any(
                    [x for x in property_types if x in card.contents[0].contents[1].contents[2].text.lower()]) else 'unknown',
                'address': self.get_address(card),
                'postcode': None,
                'postcode_district': post_code_district,
                'price': self.get_int(self.get_text(card, 'p', 'class', zoopla_class_mapping['propertyPrice']).strip()),
                # .split(' ')),
                'bathrooms': self.get_room_count(card, "bed"),
                'bedrooms': self.get_room_count(card, "bath"),
                'reception': self.get_room_count(card, "chair"),
                'image': self.get_image(card),
                'description': None,  # description is no longer on the splash page of a zoopla
            })

        pages = content.find('div', {'data-testid': 'pagination'}).contents[0].contents

        if pages:
            current_page = self.get_int([x.contents[0].text for x in list(pages)[1:-1] if "current page" in x.contents[0].attrs.get('aria-label')][0])

            if current_page == 1:
                self.max_page = self.get_int(pages[-2].text)

            if current_page == self.max_page:
                return True  # this is the last page so break while loop
        else:  # only one page for post code search break while loop
            return True

    def get_address(self, card):
        return self.get_text(card, 'p', 'class', zoopla_class_mapping['address']).strip().replace(',', ' ').replace("\"", "")

    def get_marketed_by(self, card):
        obj = card.find('a', {'data-testid': "listing-details-agent-logo"})
        if obj:
            try:
                string = obj.next_element.attrs['alt'].split(',')[0]
            except KeyError as e:
                string = obj.next_element.text
        return string.split('Marketed by ')[-1].replace(', ', '').replace("\"", '')

    def get_image(self, card):
        image = card.find('a', {'class': zoopla_class_mapping['propertyImage']}).contents[0].attrs['src']
        if image[0:5] == "https":
            return image
        else:
            return card.find('a', {'class': zoopla_class_mapping['propertyImage']}).contents[0].attrs['data-src']

    def get_room_count(self, card, ref):
        value = [self.get_int(x.attrs['content']) for x in
                card.find('div', {'class': "css-58bgfg-WrapperFeatures e2uk8e15"}).contents if
                x.find('span', {'data-testid': ref}) is not None]
        if len(value) != 0:
            return self.get_int(value[0])
        else:
            return None

    def clear_results(self):
        self.results = []

    def results_to_string(self):
        string: str = f"{', '.join(self.results[0].keys())}\n"
        for row in self.results:
            string = f"{string}{', '.join(['' if x is None else str(x) for x in row.values()])}\n"

        self.clear_results()

        return string


if __name__ == '__main__':
    scraper = ScrapZoopla()
    output = scraper.run(post_code_district='PE28', listing_type='for-sale')