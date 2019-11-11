import logging

from datetime import datetime
from urllib.parse import urlparse

from bs4 import BeautifulSoup

import utils
from RawEvent import RawEvent
from RemoteCrawler import RemoteListCrawler


logger = logging.getLogger(__name__)


class ChemistryGatechEduCrawler(RemoteListCrawler):
    IDENTIFIER = 'chemistry.gatech.edu'
    DOMAIN = 'https://www.chemistry.gatech.edu'
    URL = '/events/all'

    SELECTOR_EVENTS = '#page #main #content .view-all-events .view-content tbody tr'
    SELECTOR_EVENT_LINK = '.views-field-title a'
    SELECTOR_NEXT_PAGE = '#page #main #content .pager-next a'

    SELECTOR_EVENT_TITLE1 = '#page #main #page-title h2'
    SELECTOR_EVENT_TITLE2 = '#page #main #content .field-name-field-teaser strong'
    SELECTOR_EVENT_LOCATION = '#page #main #content .field-name-field-room-location .field-item'
    SELECTOR_EVENT_DESCRIPTION = '#page #main #content .field-name-field-event-description .field-item'
    SELECTOR_EVENT_TIMESINGLE = '#page #main #content .date-display-single'
    SELECTOR_EVENT_TIMESTART = '#page #main #content .date-display-start'
    SELECTOR_EVENT_TIMEEND = '#page #main #content .date-display-end'
    SELECTOR_EVENT_LINKS = '#page #main #content .content a'

    FORMAT_EVENT_DATESINGLE = '%A, %B %d, %Y - %I:%M%p'
    FORMAT_EVENT_DATE = '%A, %B %d, %Y'
    FORMAT_EVENT_DATE_OVERVIEW = '%B %d, %Y'
    FORMAT_EVENT_TIME = '%I:%M%p'

    LINK_PREFIX_INCLUDE = '/event/'

    def getEventList(self):
        events = set()
        lastURL = self.DOMAIN
        nextURL = self.URL

        def _getLink(elem, base):
            def _matchesEventLink(link):
                if not link:
                    return False

                try:
                    parsed = urlparse(link)
                except ValueError:
                    return False
                return (parsed.netloc == 'www.chemistry.gatech.edu' and
                        parsed.path.startswith(self.LINK_PREFIX_INCLUDE))

            links = elem.select(self.SELECTOR_EVENT_LINK)
            links = map(utils.Soup.getElemLink, links)
            links = map(utils.normalizeURL(base=base), links)
            links = filter(_matchesEventLink, links)
            return utils.firstOrNone(links)

        while 1:
            nextURL = utils.normalizeURL(base=lastURL, url=nextURL)
            overview = self.requester.fetchURL(nextURL)
            if not overview:
                break

            soup = BeautifulSoup(overview, 'html.parser')
            evs = soup.select(self.SELECTOR_EVENTS)
            evs = map(lambda l: _getLink(l, nextURL), evs)
            events|= set(filter(None, evs))

            lastURL = nextURL
            nextURL = utils.Soup.getLinkAt(soup, self.SELECTOR_NEXT_PAGE)
            if not nextURL:
                break
        return events

    def _getEventDetails(self, eventURL):
        details = self.requester.fetchURL(eventURL)
        if not details:
            return None

        soup = BeautifulSoup(details, 'html.parser')

        title1 = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_TITLE1)
        title2 = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_TITLE2)
        if title1 and title2:
            title = '%s: %s' % (title1, title2)
        else:
            title = utils.firstOrNone(filter(None, (title1, title2)))

        singleStr = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_TIMESINGLE)
        startStr = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_TIMESTART)
        endStr = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_TIMEEND)
        startTime, endTime = self._parseEventTime(singleStr, startStr, endStr)
        startTime = utils.normalizeDate(startTime,
                                        self.config['defaults']['timezone'])

        event = RawEvent(self.IDENTIFIER, eventURL, title, startTime)
        event.setEnd(utils.normalizeDate(endTime, 
                                         self.config['defaults']['timezone']))
        event.setLocation(utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_LOCATION))

        description, links = utils.Soup.tokenizeElemAt(soup,
                                                       self.SELECTOR_EVENT_DESCRIPTION,
                                                       base=eventURL)
        links|= set(utils.Soup.getLinksAt(soup, self.SELECTOR_EVENT_LINKS))
        links = map(utils.normalizeURL(base=eventURL), links)
        links = set(filter(None, links))
        event.setDescription(description)
        event.setLinks(links)

        return event

    def _parseEventTime(self, singleStr, startStr, endStr):
        startTime, endTime = None, None
        if singleStr:
            splits = singleStr.split(' to ')
            if len(splits) != 2:
                logger.error(f'Unknown date string format {singleStr}')
                return None, None

            startStr, endStr = map(str.strip, splits)
            try:
                startTime = datetime.strptime(startStr,
                                              self.FORMAT_EVENT_DATESINGLE)
                endTime = datetime.strptime(endStr, self.FORMAT_EVENT_TIME)
            except ValueError as e:
                logger.error(f'Cannot parse date string {startStr} and {endStr}')
                logger.exception(e)
                return startTime, endTime
            return startTime, startTime.replace(hour=endTime.hour,
                                                minute=endTime.minute)

        if not startStr:
            return None, None

        try:
            startTime = datetime.strptime(startStr, self.FORMAT_EVENT_DATESINGLE)
            endTime = datetime.strptime(endStr, self.FORMAT_EVENT_DATESINGLE)
        except ValueError:
            logger.error(f'Cannot parse date string {startStr} and {endStr}')
            logger.exception(e)

        return startTime, endTime

