import logging

from datetime import datetime

from bs4 import BeautifulSoup

import utils
from RawEvent import RawEvent
from RemoteCrawler import RemoteListCrawler


logger = logging.getLogger(__name__)


class CampuslabsComCrawler(RemoteListCrawler):
    IDENTIFIER = 'gatech.campuslabs.com'
    DOMAIN = 'https://gatech.campuslabs.com'
    URL = '/engage/api/discovery/event/search'
    EVENT_URL = '/engage/event/{}'

    LIST_REQUEST_DATA = {
        'endsAfter': '',
        'orderByField': 'endsOn',
        'orderByDirection': 'ascending',
        'status': 'Approved',
        'take': 10000,
        'query': '',
    }

    DESCRIPTION_STYLE = {
        'FORMAT_PREPEND': {
            'blockquote': '\n\n',
        },
        'FORMAT_APPEND': {
            'blockquote': '\n',
            'div': '\n\n'
        }
    }

    def getEventList(self):
        url = utils.normalizeURL(base=self.DOMAIN, url=self.URL)
        reqData = self.LIST_REQUEST_DATA.copy()
        backlogTime = datetime.now() - self.config['backlog']
        backlogTime = self.config['defaults']['timezone'].localize(backlogTime)
        reqData['endsAfter'] = backlogTime.replace(microsecond=0).isoformat()
        res = self.requester.fetchURL(url, data=reqData, json=True)

        events = set()
        for event in res['value']:
            eventURL = utils.normalizeURL(base=self.DOMAIN, url=self.EVENT_URL)
            eventURL = eventURL.format(event['id'])

            startTime = datetime.fromisoformat(event['startsOn'])
            startTime = utils.normalizeDate(startTime,
                                            self.config['defaults']['timezone'])

            rawEvent = RawEvent(self.IDENTIFIER,
                                eventURL,
                                event['name'],
                                startTime)

            if 'endsOn' in event:
                endTime = datetime.fromisoformat(event['endsOn'])
                endTime = utils.normalizeDate(endTime,
                                              self.config['defaults']['timezone'])
                rawEvent.setEnd(endTime)

            rawEvent.setLocation(event['location'])
            rawEvent.setExtras(', '.join(event.get('benefitNames', ())))

            soup = BeautifulSoup(event['description'], 'html.parser')
            description, links = utils.HTMLToText.tokenizeSoup(soup,
                                                               base=url,
                                                               customStyle=self.DESCRIPTION_STYLE)
            links = map(utils.normalizeURL(base=eventURL), links)
            links = set(filter(None, links))

            rawEvent.setDescription(description)
            rawEvent.setLinks(links)

            rawEvent.setStatus(event['status'])
            events.add(rawEvent)

        return events
