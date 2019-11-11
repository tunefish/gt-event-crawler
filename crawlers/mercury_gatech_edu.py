import json
import logging

from datetime import datetime, timedelta

from bs4 import BeautifulSoup

import utils
from RawEvent import RawEvent
from RemoteCrawler import RemoteListCrawler


logger = logging.getLogger(__name__)


class MercuryGatechEduCrawler(RemoteListCrawler):
    IDENTIFIER = 'hg.gatech.edu'
    DOMAIN = 'http://hg.gatech.edu'
    URL = '/views/ajax'

    # lists all entries updated since a given unix timestamp
    UPDATED_URL = '/uptracker/json/{}'

    # url to resolve an event given its ID
    EVENT_URL = '/node/{}'

    # extracted from ajax calls
    LIST_REQUEST_DATA = {
        'type_1': 'event',
        'title': '',
        'view_name': 'content_tools',
        'view_display_id': 'block',
        'view_args': '',
        'pager_element': 0,
        'ajax_html_ids': [
            # this list is not strictly required by the server
            # but we'll keep it not to break anything
            'skip-link',
            'header',
            'logoWrapper',
            'gtLogo',
            'mercuryLogo',
            'navbar-block-block',
            'breadcrumb',
            'meat',
            'blubber',
            'leftNav',
            'block-gt-cas-tools-gt-cas-login',
            'CASlogoutBlockLink',
            'rightNav',
            'content',
            'block-system-main',
            'node-385808',
            'block-views-content-tools-block',
            'views-exposed-form-content-tools-block',
            'edit-type-1-wrapper',
            'edit-type-1',
            'edit-title-wrapper',
            'edit-title',
            'edit-submit-content-tools',
            'footer',
        ],
        'page': 1,
        'order': 'created',
        'sort': 'desc',
    }

    SELECTOR_EVENT = '.view .view-content .views-table tbody tr'
    SELECTOR_EVENT_POSTED = 'td.views-field.views-field-created'
    SELECTOR_NEXT_PAGE = '.view .item-list .pager-next'

    SELECTOR_TYPE = 'type'
    SELECTOR_EVENT_TITLE = 'title'
    SELECTOR_EVENT_DESCRIPTION = 'body'
    SELECTOR_EVENT_AUDIENCE = 'field_audience item value'
    SELECTOR_EVENT_EXTRAS = 'field_extras item value'
    SELECTOR_EVENT_LINKS = 'links_related item url'
    SELECTOR_EVENT_LOCATION = 'field_location item value'
    SELECTOR_EVENT_URLS = 'field_url item url'

    SELECTOR_EVENT_HTMLTIMES = '#meat #content .content .eventWrapper .detailsSidebar .allTimesList li'
    SELECTOR_EVENT_HTMLMETADATA = '#meat #content .content .eventWrapper .metaDataList'

    FORMAT_POSTED = '%a, %b %d, %Y - %I:%M%p'
    FORMAT_EVENT_DATE = '%A %B %d, %Y'
    FORMAT_EVENT_TIME = '%I:%M %p'

    def getEventList(self):
        url = utils.normalizeURL(base=self.DOMAIN, url=self.URL)
        lastPosted = datetime.now()
        searchBacklog = timedelta(days=self.config['search_backlog'])
        crawlUntil = lastPosted - searchBacklog
        page = 0

        events = set()
        while lastPosted > crawlUntil:
            reqData = self.LIST_REQUEST_DATA.copy()
            reqData['page'] = page
            eventList = self.requester.fetchURL(url, method='POST', data=reqData)

            if not eventList:
                break

            res = json.loads(eventList)
            if (not isinstance(res, list)
                or self._getHTMLInsertEntry(res) is None):
                logger.error(f'Malformed response from {url}: {eventList}')
                break

            res = self._getHTMLInsertEntry(res).get('data', '')
            soup = BeautifulSoup(res, 'html.parser')

            for event in soup.select(self.SELECTOR_EVENT):
                link = utils.Soup.getLinkAt(event, 'a')
                if link:
                    normalized = utils.normalizeURL(base=url, url=link)
                    if normalized:
                        events.add(normalized)

                posted = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_POSTED)
                if posted:
                    try:
                        postedTime = datetime.strptime(posted, self.FORMAT_POSTED)
                        lastPosted = min(lastPosted, postedTime)
                    except ValueError:
                        logger.warning('Unable to parse posted time {posted}')
                        lastPosted+= timedelta(days=1)

            if not soup.select(self.SELECTOR_NEXT_PAGE):
                break

            page+= 1

        updatesSince = datetime.now() - searchBacklog
        requestTimestamp = int(updatesSince.timestamp())
        url = self.UPDATED_URL.format(requestTimestamp)
        url = utils.normalizeURL(base=self.DOMAIN, url=url)
        updatedEventList = self.requester.fetchURL(url, json=True)

        if updatedEventList is not None:
            for eventId in updatedEventList:
                url = self.EVENT_URL.format(eventId)
                events.add(utils.normalizeURL(base=self.DOMAIN, url=url))
        else:
            logger.warning('Cannot fetch recently updated events!')

        return events

    def _getEventDetails(self, eventURL):
        details = self.requester.fetchURL(f'{eventURL}/xml', errorOnCode=(403,))
        if not details:
            return None

        soup = BeautifulSoup(details, 'xml')
        if utils.Soup.getTextAt(soup, self.SELECTOR_TYPE) != 'event':
            return set()
        title = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_TITLE)
        location = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_LOCATION)

        description = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_DESCRIPTION)
        links = set()
        if description:
            descSoup = BeautifulSoup(description, 'html.parser')
            description, links = utils.HTMLToText.tokenizeSoup(descSoup,
                                                               base=eventURL)

        relatedLinks = utils.Soup.getTextsAt(soup, self.SELECTOR_EVENT_LINKS)
        if relatedLinks:
            links|= set(relatedLinks)

        urls = utils.Soup.getTextsAt(soup, self.SELECTOR_EVENT_URLS)
        if urls:
            links|= set(urls)
        links = map(utils.normalizeURL(base=eventURL), links)
        links = set(filter(None, links))

        audience = utils.Soup.getTextsAt(soup, self.SELECTOR_EVENT_AUDIENCE)
        if audience:
            audience = ', '.join(audience)

        extras = utils.Soup.getTextsAt(soup, self.SELECTOR_EVENT_EXTRAS)
        if extras:
            def _cleanExtra(xtr):
                return xtr.replace('_', ' ').capitalize()
            extras = ', '.join(map(_cleanExtra, extras))

        # need to fetch human readable site because XML does not contain status
        status = None
        details2 = self.requester.fetchURL(eventURL, errorOnCode=(403,))
        soup2 = BeautifulSoup(details2 or '', 'lxml')
        if details2:
            # HTML tree is broken sometimes, which will confuse the python parser
            statusBlock = self._getBlockFromList(soup2,
                                                 self.SELECTOR_EVENT_HTMLMETADATA,
                                                 'status')

            if statusBlock:
                statusElem = self._getBlockDetail(statusBlock, 'workflow status')
                status = utils.Soup.getElemText(statusElem)
                status = status.lower()

        # parse dates from HTML, because timezones and recurring events are
        # entirely messed up in the XML (some rrules are really weird and in no
        # way generate the event instances listed in HTML, eg http://hg.gatech.edu/node/623952)
        htmlTimes = soup2.select(self.SELECTOR_EVENT_HTMLTIMES)

        events = set()
        for n, timeEntry in enumerate(htmlTimes):
            startTime, endTime = self._parseHTMLEventTime(timeEntry)
            if not startTime:
                logger.error(f'Cannot parse event time for {eventURL}: {utils.Soup.getElemText(timeEntry)}')
                continue

            rawEvent = RawEvent(self.IDENTIFIER,
                                f'{eventURL}#{n}',
                                title,
                                startTime)
            rawEvent.setEnd(endTime)
            rawEvent.setDescription(description)
            rawEvent.setLinks(links)
            if location:
                rawEvent.setLocation(location)
            if audience:
                rawEvent.setAudience(audience)
            if extras:
                rawEvent.setExtras(extras)
            if status:
                rawEvent.setStatus(status)
            events.add(rawEvent)
        return events

    def _parseHTMLEventTime(self, timeEntry):
        strings = utils.Soup.getChildrenTexts(timeEntry)
        startTime, endTime = None, None
        if len(strings) == 2:
            dateStr, timeStr = strings

            sDate, eDate = self._parseDateComponent(dateStr)
            if not sDate or not eDate:
                return None, None

            allDay, sTime, eTime = self._parseTimeComponent(timeStr)
            if allDay:
                # the end is at the beginning of <eDate>, hence +1
                sDate = sDate.date()
                eDate = (eDate + timedelta(days=1)).date()

                # date objects do not relate to a timezone so no normalization required
                return sDate, eDate
            else:
                if not sTime:
                    return None, None
                sTime = sDate.replace(hour=sTime.hour, minute=sTime.minute)
                if eTime:
                    eTime = eDate.replace(hour=eTime.hour, minute=eTime.minute)

            return (utils.normalizeDate(sTime, self.config['defaults']['timezone']),
                    utils.normalizeDate(eTime, self.config['defaults']['timezone']))
        else:
            logger.error(f'Unable to parse unknown datetime format {timeEntry.text}')
            return None, None

    def _parseDateComponent(self, dateStr):
        try:
            if dateStr.count('-') == 1:
                # <start day> - <end day>
                startStr, endStr = map(str.strip, dateStr.split('-'))
                startDate = datetime.strptime(startStr, self.FORMAT_EVENT_DATE)
                endDate = datetime.strptime(endStr, self.FORMAT_EVENT_DATE)
            elif '-' not in dateStr:
                # <day>
                startDate = datetime.strptime(dateStr, self.FORMAT_EVENT_DATE)
                endDate = startDate
            else:
                raise ValueError(f'Unable to parse unknown datetime format {timeEntry.text}')
        except ValueError as e:
            logger.error(f'Unable to parse date string {dateStr}')
            logger.exception(e)
            return None, None

        return startDate, endDate

    def _parseTimeComponent(self, timeStr):
        try:
            if timeStr.lower() == 'all day':
                return True, None, None
            else:
                if timeStr.count('-') == 1:
                    # <start time> - <end time>
                    startStr, endStr = map(str.strip, timeStr.split('-'))
                    startTime = datetime.strptime(startStr,
                                                  self.FORMAT_EVENT_TIME)
                    endTime = datetime.strptime(endStr,
                                                self.FORMAT_EVENT_TIME)
                elif '-' not in timeStr:
                    # <start time>
                    startTime = datetime.strptime(timeStr, self.FORMAT_EVENT_TIME)
                    endTime = None
                else:
                    raise ValueError(f'Unable to parse unknown datetime format {timeEntry.text}')

                return False, startTime, endTime
        except ValueError as e:
            logger.error(f'Unable to parse time string {timeStr}')
            logger.exception(e)
            return False, None, None

    def _getHTMLInsertEntry(self, response):
        def _isInsertEntry(x):
            return isinstance(x, dict) and x.get('command', '') == 'insert'
        return next(filter(_isInsertEntry, response), None)

    def _getBlockFromList(self, soup, selector, blockName):
        elems = utils.firstOrNone(soup.select(selector))
        if not elems:
            logger.warning(f'No blocklist {selector}')
            return None

        def _isBlock(elem):
            return elem.text.strip().lower() == blockName

        detailsLabel = elems.findChild(_isBlock)
        if not detailsLabel:
            logger.warning(f'No label {blockName}')
            return None

        return detailsLabel.findNextSibling()

    def _getBlockDetail(self, detailSoup, detail, onlyNextElem=True):
        def _isDetailLabel(elem):
            return (elem.name == 'strong'
                    and elem.text.strip().lower()[:-1] == detail)

        def _validTextElement(elem):
            return not elem.name == 'br' and utils.Soup.getElemText(elem)

        label = detailSoup.findChild(_isDetailLabel)
        if not label:
            return None

        if onlyNextElem:
            elems = filter(_validTextElement, label.nextSiblingGenerator())
            return next(elems, None)
        else:
            return label.parent
