import logging

from datetime import datetime

from bs4 import BeautifulSoup

import utils
from RawEvent import RawEvent
from RemoteCrawler import RemoteListCrawler


logger = logging.getLogger(__name__)


class CareerGatechEduCrawler(RemoteListCrawler):
    IDENTIFIER = 'career.gatech.edu'
    DOMAIN = 'https://career.gatech.edu'
    URL = '/employer-information-sessions'
    SELECTOR_EVENTS = '#page #main #content .content .info-session .content'
    SELECTOR_EVENT_TITLE = 'h4'
    SELECTOR_EVENT_STARTTIME = '.date-time'
    SELECTOR_EVENT_LOCATION = '.location'
    SELECTOR_EVENT_DESCRIPTION = '.body'
    SELECTOR_MAJORS = '.majors'

    FORMAT_EVENT_DATE = '%b %d, %Y, %I:%M %p'

    def getEventList(self):
        overview = self.requester.fetchURL(self.URL, base=self.DOMAIN)
        if not overview:
            return set()

        soup = BeautifulSoup(overview, 'html.parser')
        events = soup.select(self.SELECTOR_EVENTS)
        return set(self._getEventDetailsFromOverview(event, n)
                   for n, event in enumerate(events))

    def _getEventDetailsFromOverview(self, event, n):
        url = utils.normalizeURL(base=self.DOMAIN, url=self.URL)
        title = utils.Soup.getTextAt(event, self.SELECTOR_EVENT_TITLE)

        startStr = utils.Soup.getTextAt(event, self.SELECTOR_EVENT_STARTTIME)
        if not startStr:
            logger.error(f'Cannot find start time on page {url}')
            return None
        startTime = datetime.strptime(startStr, self.FORMAT_EVENT_DATE)
        startTime = utils.normalizeDate(startTime,
                                        self.config['defaults']['timezone'])

        rawEvent = RawEvent(self.IDENTIFIER, f'{url}#{n}', title, startTime)

        location = utils.Soup.getTextAt(event, self.SELECTOR_EVENT_LOCATION)
        rawEvent.setLocation(location)

        majors = utils.Soup.getTextAt(event, self.SELECTOR_MAJORS)
        if majors.lower().startswith('majors'):
            majors = majors[6:]
        rawEvent.setAudience(majors)

        description, links = utils.Soup.tokenizeElemAt(event,
                                                       self.SELECTOR_EVENT_DESCRIPTION,
                                                       base=url)
        links = map(utils.normalizeURL(base=self.URL), links)
        links = set(filter(None, links))

        rawEvent.setDescription(description)
        rawEvent.setLinks(links)

        return rawEvent
