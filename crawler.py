#!/usr/bin/env python3
import argparse
import functools
import json
import logging
import operator
import os

import icalendar
import pytz

from base64 import b64decode
from collections import defaultdict
from configparser import ConfigParser
from datetime import datetime, date, time, timedelta
from itertools import chain
from time import monotonic as time_monotonic
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from github import Github, GithubException
from github import UnknownObjectException as UnknownGithubObjectException

import utils


logging.basicConfig()
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

logger = logging.getLogger(__name__)


class CrawlerConfig:
    __slots__ = ('userAgent', 'logLevel', 'retries', 'retryTimeout', 'timezone',
                 'eventLength', 'backlog', 'mercurySearchBacklog',
                 'maxTokens', 'tokenRate', 'crawlerWeights', 'statusCodes',
                 'repository', 'jsonFile', 'icsFile')

    @classmethod
    def fromIni(cls, path):
        cp = ConfigParser()
        cp.read(path)

        def _stringList(s):
            return tuple(map(str.strip, s.lower().split(',')))

        cfg = cls()

        # general
        cfg.userAgent = cp['general']['user_agent']
        cfg.logLevel = cp['general']['log_level'].upper()

        # crawler defaults
        defaults = cp['crawler_defaults']
        cfg.retries = int(defaults['retries'])
        cfg.retryTimeout = int(defaults['retry_timeout'])
        cfg.timezone = pytz.timezone(defaults['timezone'])
        cfg.eventLength = timedelta(minutes=int(defaults['event_length']))
        cfg.backlog = timedelta(days=int(defaults['backlog']))
        cfg.mercurySearchBacklog = timedelta(days=int(defaults['mercury_search_backlog']))

        # throttling
        cfg.maxTokens = int(cp['throttling']['max_tokens'])
        cfg.tokenRate = float(cp['throttling']['token_rate'])

        # crawler weights
        weights = cp['crawler_weights']
        cfg.crawlerWeights = {crawler: int(weight)
                              for crawler, weight in weights.items()}

        # status codes
        cfg.statusCodes = {status.lower(): int(code)
                           for status, code in cp['status_codes'].items()}

        # storage
        cfg.repository = cp['storage']['repository']
        cfg.jsonFile = cp['storage']['json_file']
        cfg.icsFile = cp['storage']['ics_file']

        return cfg


class RawEvent:
    BASE_TZ = pytz.utc
    __slots__ = ('crawler', 'url', 'alternativeUrls', 'title', 'start', 'end',
                 'location', 'extras', 'description', 'links', 'audience',
                 'status')

    def __init__(self, crawler, url, title, start,
                 alternativeUrls=None, end=None,
                 location=None, extras=None, description=None, links=None,
                 audience=None, status=None):
        self.crawler = crawler
        self.url = url

        self.setTitle(title)
        self.setStart(start)

        self.alternativeUrls = alternativeUrls
        self.end = end
        self.location = location
        self.extras = extras
        self.description = description or ()
        self.links = links or frozenset()
        self.audience = audience
        self.status = status

    def setAlternativeUrls(self, urls):
        self.alternativeUrls = urls

    def setTitle(self, title):
        if not title:
            raise ValueError('Event must have a title!')
        self.title = title

    def setStart(self, start):
        if not start:
            raise ValueError('Event must have a start time!')
        self.start = start

    def setEnd(self, end):
        self.end = end

    def setLocation(self, location):
        self.location = location

    def setExtras(self, extras):
        self.extras = utils.sortStringList(extras)

    def setDescription(self, description):
        self.description = description

    def setLinks(self, links):
        self.links = links

    def setAudience(self, audience):
        self.audience = utils.sortStringList(audience)

    def setStatus(self, status):
        self.status = status.lower()

    def getEnd(self, base=None, delta=None):
        if self.end:
            return self.end

        base = base or self.start
        if delta:
            return base + delta
        elif not isinstance(base, datetime):
            return base + timedelta(days=1)
        else:
            return base + timedelta(hours=1)

    def getDescription(self):
        if not self.description:
            return ''

        sLinks = (self.url, *(sorted(self.alternativeUrls or ())))
        description, _ = utils.HTMLToText.renderTokens(self.description,
                                                       (),
                                                       specialLinks=sLinks)
        return description

    def include(self, other):
        altUrls = ((self.alternativeUrls or frozenset())
                   | frozenset((other.url,))
                   | (other.alternativeUrls or frozenset()))
        self.setAlternativeUrls(altUrls)

        if (not self.location
            or (other.location
                and self.location.lower() in other.location.lower())):
            # update location if other.location is more specific
            self.setLocation(other.location)

        if not self.extras:
            self.setExtras(other.extras)
        elif other.extras:
            self.setExtras(utils.mergeStringLists(self.extras, other.extras))

        if not self.description:
            self.setDescription(other.description)
        elif other.description:
            # get rid of formatting, we only care about the unformatted text
            desc = self.getDescription()
            otherDesc = other.getDescription()
            if utils.isSubsequenceByWord(desc, otherDesc):
                self.setDescription(other.description)
            elif not utils.isSubsequenceByWord(otherDesc, desc):
                logger.warning(f'The event {self} from {self.url} and {other.url} have different descriptions!')
                self.setDescription((*self.description,
                                     '\n\n',
                                     *other.description))

        self.setLinks(self.links | (other.links or frozenset()))

        if not self.audience:
            self.setAudience(other.audience)
        elif other.audience:
            self.setAudience(utils.mergeStringLists(self.audience, other.audience))

        if not self.status:
            self.setStatus(other.status)

        return self

    def toICAL(self, defaultEventLength=None):
        event = icalendar.Event()
        event['UID'] = self.url
        event['SUMMARY'] = self.title
        event.add('DTSTART', self.start)
        event.add('DTEND', self.getEnd(delta=defaultEventLength))
        event['LOCATION'] = self.location
        event['DESCRIPTION'] = self._renderDescription()
        return event

    def toJSON(self):
        return {
            'crawler':         self.crawler,
            'url':             self.url,
            'title':           self.title,
            'start':           self.start.isoformat(),
            'end':             self.end.isoformat() if self.end else None,
            'location':        self.location,
            'extras':          self.extras,
            'description':     utils.HTMLToText.stringifyTokens(self.description),
            'links':           sorted(self.links or ()),
            'audience':        self.audience,
            'status':          self.status,
        }

    @classmethod
    def fromJSON(cls, jObj):
        event = cls(jObj['crawler'],
                    jObj['url'],
                    jObj['title'],
                    utils.parseIsoDateOrDatetime(jObj['start']))
        if jObj['end']:
            event.setEnd(utils.parseIsoDateOrDatetime(jObj['end']))
        if jObj['location']:
            event.setLocation(jObj['location'])
        if jObj['extras']:
            event.setExtras(jObj['extras'])
        if jObj['description']:
            description = utils.HTMLToText.unstringifyTokens(jObj['description'])
            event.setDescription(description)
        if jObj['links']:
            event.setLinks(frozenset(jObj['links']))
        if jObj['audience']:
            event.setAudience(jObj['audience'])
        if jObj['status']:
            event.setStatus(jObj['status'])
        return event

    def _renderDescription(self):
        audience = f'Intended audience: {self.audience}' if self.audience else ''
        extras = f'Extras: {self.extras}' if self.extras else ''
        status = f'Status: {self.status or "unknown"}'
        meta = '\n'.join(filter(None, (audience, extras, status)))

        sLinks = (self.url, *(sorted(self.alternativeUrls or ())))
        description, links = utils.HTMLToText.renderTokens(self.description or (),
                                                           self.links or (),
                                                           specialLinks=sLinks)
        links = f'Links:\n{links}'
        return '\n\n'.join(filter(None, (meta, description, links)))

    def refDate(self):
        refDate = utils.getDatetime(self.end or self.start)
        # time zone information is not relevant here
        if isinstance(refDate, datetime):
            refDate = refDate.replace(tzinfo=None)
        return refDate

    def _cleanTitle(self):
        return ''.join(filter(str.isalnum, self.title.lower()))

    def definingTuple(self):
        # This tuple defines the uniqueness of an event for deduplication
        return (self._cleanTitle(),
                utils.getDatetime(self.start),
                utils.getDatetime(self.getEnd()))

    def _compTuple(self):
        # This tuple should be used for sorting events (by date, title, source)
        start = utils.getDatetime(self.start)
        if not start.tzinfo:
            start = self.BASE_TZ.localize(start)

        end = utils.getDatetime(self.getEnd(base=start))
        if not end.tzinfo:
            end = self.BASE_TZ.localize(end)
        return start, end, self._cleanTitle(), self.url

    def __eq__(self, other):
        return (isinstance(other, RawEvent)
                and self.definingTuple() == other.definingTuple())

    def __lt__(self, other):
        return self._compTuple() < other._compTuple()

    def __hash__(self):
        return hash(self.definingTuple())

    def __str__(self):
        start = self.start
        end = self.end
        if isinstance(self.start, datetime):
            start = self.start.astimezone(RawEvent.BASE_TZ)
        end = self.getEnd(base=start)

        if start and end:
            sDate = utils.getDate(start)
            eDate = utils.getDate(end)

            if not isinstance(start, datetime):
                evDate = start.strftime('%B %d %Y')
            else:
                evDate = start.strftime('%B %d %Y from %H:%M')

            if sDate == eDate:
                if isinstance(end, datetime):
                    evDate+= end.strftime(' to %H:%M')
            else:
                if not isinstance(end, datetime):
                    evDate+= end.strftime(' to %B %d %Y')
                else:
                    evDate+= end.strftime(' to %B %d %Y at %H:%M')
        elif start:
            if not isinstance(start, datetime):
                evDate = start.strftime('%B %d %Y')
            else:
                evDate = start.strftime('%B %d %Y at %H:%M')
        else:
            evDate = '<unknown date>'
        extra = f' ({self.extras})' if self.extras else ''
        return f'{self.title}, at {self.location or "<somewhere>"} on {evDate}{extra}'

    def __repr__(self):
        return f'<{str(self)}>'


class DedupEventSet:
    def __init__(self, statusCodes=None, crawlerWeights=None):
        self.definingTuples = defaultdict(list)
        self.eventUrls = {}
        self._statusCodes = statusCodes or {}
        self._crawlerWeights = crawlerWeights or {}

    def addEvent(self, event, update=True):
        updated = True

        if event.url in self.eventUrls:
            if not update:
                logger.debug(f'Not adding duplicate event (by  URL) {event.url}')
                return False

            # remove old event
            oldEvent = self.eventUrls[event.url]
            updated = event.toICAL() != oldEvent.toICAL()
            logger.debug(f'Remove {oldEvent} from {event.url}: crawled URL again: {event}')
            self.removeEvent(oldEvent)

        self.definingTuples[event.definingTuple()].append(event)
        self.eventUrls[event.url] = event
        for url in (event.alternativeUrls or ()):
            self.eventUrls[url] = event

        return updated

    def removeEvent(self, event):
        self.definingTuples[event.definingTuple()].remove(event)
        del self.eventUrls[event.url]
        for url in (event.alternativeUrls or ()):
            del self.eventUrls[url]

    def __len__(self):
        return len(self.definingTuples.values())

    def __iter__(self):
        def _sortByWeight(event):
            statusCodeWeight = self._statusCodes.get(event.status, 25)
            crawlerWeight = self._crawlerWeights.get(event.crawler, 25)
            return (statusCodeWeight, crawlerWeight, event.url)

        for _, events in self.definingTuples.items():
            if events:
                event, *events = sorted(events, key=_sortByWeight, reverse=True)
                if events:
                    logger.info(f'Merging {event} from {event.url} with instances at {tuple(x.url for x in events)}')
                for mergeEvent in events:
                    event.include(mergeEvent)
                yield event

    def iterUniqueEvents(self):
        return iter(self)

    def iterAllEvents(self):
        return chain(*self.definingTuples.values())


class Crawler:
    def __init__(self, config):
        self.config = config
        self.calendars = list()
        self.events = defaultdict(set)
        self.crawledEvents = DedupEventSet(statusCodes=config.statusCodes,
                                           crawlerWeights=config.crawlerWeights)

        self.requester = utils.Requester(self.config.userAgent,
                                         self.config.retries,
                                         self.config.retryTimeout,
                                         self.config.maxTokens,
                                         self.config.tokenRate)
        self._hasCrawledNewEvent = False

    def registerCalendarCrawler(self, crawler):
        if isinstance(crawler, RemoteCalendarCrawler):
            # crawler has already been instantiated, just add it
            if crawler not in self.calendars:
                self.calendars.append(crawler)
        elif (isinstance(crawler, type)
              and issubclass(crawler, RemoteCalendarCrawler)):
            if not any(c.__class__ == crawler for c in self.calendars):
                self.calendars.append(crawler(self.config, self.requester))
        else:
            raise ValueError(f'{crawler} is not a calendar crawler (RemoteCalendarCrawler)')

    def discover(self, skipExceptions=3):
        logger.info('Start fetching events for each calendar')
        stats = {cal: 0 for cal in self.calendars}

        def _handleError(calendar, e):
            logger.error(f'An error occurred while trying to obtain a list of events for calendar {calendar.IDENTIFIER}')
            logger.exception(e)

        t_start = time_monotonic()
        for calendar in self.calendars:
            exceptionCount = 0
            calDate = datetime.now()
            events = None
            t_calendar = time_monotonic()

            if isinstance(calendar, RemoteListCrawler):
                try:
                    self.events[calendar]|= calendar.getEventList()
                except Exception as e:
                  _handleError(calendar, e)
            elif isinstance(calendar, RemoteMonthCrawler):
                while exceptionCount < skipExceptions:
                    try:
                        events = calendar.getMonthEventList(year=calDate.year,
                                                            month=calDate.month)
                        logger.debug(f'Found {len(events)} events this round for {calendar.IDENTIFIER}')
                    except Exception as e:
                        _handleError(calendar, e)
                        exceptionCount+= 1
                    if not events:
                        break
                    self.events[calendar]|= events
                    calDate = utils.addMonths(calDate, 1)
            elif isinstance(calendar, RemoteWeekCrawler):
                while exceptionCount < skipExceptions:
                    try:
                        week = calDate.isocalendar()[1]
                        events = calendar.getWeekEventList(year=calDate.year,
                                                           week=week)
                        logger.debug(f'Found {len(events)} events this round for {calendar.IDENTIFIER}')
                    except Exception as e:
                        _handleError(calendar, e)
                        exceptionCount+= 1
                    if not events:
                        break
                    self.events[calendar]|= events
                    calDate = calDate + timedelta(days=7)
            elif isinstance(calendar, RemoteDayCrawler):
                while exceptionCount < skipExceptions:
                    try:
                        events = calendar.getDayEventList(year=calDate.year,
                                                          month=calDate.month,
                                                          day=calDate.day)
                        logger.debug(f'Found {len(events)} events this round for {calendar.IDENTIFIER}')
                    except Exception as e:
                        _handleError(calendar, e)
                        exceptionCount+= 1
                    if not events:
                        break
                    self.events[calendar]|= events
                    calDate += timedelta(days=1)
            else:
                logger.error(f'Unknown calendar type {type(calendar)}')
                continue

            stats[calendar]+= time_monotonic() - t_calendar
            logger.debug(f'Found {len(self.events[calendar])} events in total for {calendar.IDENTIFIER}')

        t_end = time_monotonic()
        logger.info(f'Event discovery took {t_end - t_start}s')

        tableData = tuple((cal.IDENTIFIER, len(self.events[cal]), stats[cal])
                          for cal in self.calendars)
        headers = ('Calendar', 'Events', 'Processing Time (s)')
        footers = ('Total',
                   sum(map(len, self.events.values())),
                   sum(stats.values()))
        logger.info('Statistics:\n' + utils.tabulateWithHeaderFooter(tableData,
                                                                     headers,
                                                                     footers))

    def resolve(self, timeout=0):
        t_start = time_monotonic()
        stats = {cal: {'fail': 0,
                       'success': 0,
                       'events': len(self.events[cal]),
                       'time': 0}
                 for cal in self.calendars}

        logger.info('Start fetching event details')
        hasEvents = True
        while hasEvents:
            hasEvents = False
            for calendar in self.calendars:
                t_calendar = time_monotonic()
                if self.events[calendar]:
                    hasEvents = True
                    ev = self.events[calendar].pop()
                    if ev is None:
                        loggger.warning(f'{calendar.IDENTIFIER} got None event!')
                        continue

                    try:
                        if not isinstance(ev, RawEvent):
                            ev = calendar.getEventDetails(ev)
                        if ev is not None:
                            if isinstance(ev, RawEvent):
                                self._addCrawledEvent(ev)
                                stats[calendar]['success']+= 1
                            elif isinstance(ev, (set, frozenset, tuple, list)):
                                for event in ev:
                                    self._addCrawledEvent(event)
                                stats[calendar]['success']+= 1 if ev else 0
                        else:
                            stats[calendar]['fail']+= 1
                    except Exception as e:
                        logger.error(f'An error occurred while trying to obtain the details of event {ev}')
                        logger.exception(e)
                        stats[calendar]['fail']+= 1
                stats[calendar]['time']+= time_monotonic() - t_calendar

        t_end = time_monotonic()
        logger.info(f'Event resolution took {t_end - t_start}s')

        headers = ('Calendar', 'Failure', 'Success', 'Total', 'Processing Time (s)')
        footers = ('Total',
                   sum(map(operator.itemgetter('fail'), stats.values())),
                   sum(map(operator.itemgetter('success'), stats.values())),
                   sum(map(operator.itemgetter('events'), stats.values())),
                   sum(map(operator.itemgetter('time'), stats.values())))
        tableData = tuple((cal.IDENTIFIER, *stats[cal].values())
                          for cal in self.calendars)
        logger.info('Statistics:\n' + utils.tabulateWithHeaderFooter(tableData,
                                                                     headers,
                                                                     footers))

    def _addCrawledEvent(self, event):
        if event.status and event.status not in self.config.statusCodes:
            logger.warning(f'Event at {event.url} has unknown status "{event.status}"!')
        if datetime.now() - event.refDate() > self.config.backlog:
            # don't add events we don't care about anyways
            return

        if not self.config.statusCodes.get((event.status or '').lower(), 25):
            # don't add events with bad status, eg cancelled
            logger.info(f'Skipping {event.url}: status "{event.status}"')
            return

        self._hasCrawledNewEvent|= self.crawledEvents.addEvent(event,
                                                               update=False)

    def importJSON(self, previousJson):
        numCrawledEvents = len(self.crawledEvents)

        now = datetime.now()
        for event in previousJson:
            event = RawEvent.fromJSON(event)
            if now - event.refDate() <= self.config.backlog:
                self.crawledEvents.addEvent(event)
        logger.info(f'Importing {len(self.crawledEvents) - numCrawledEvents} events.')

    def exportJSON(self, force=False, eventFilter=None):
        if not self._hasCrawledNewEvent and not force:
            return False

        exportEvents = filter(eventFilter, self.crawledEvents.iterAllEvents())
        return tuple(event.toJSON() for event in sorted(exportEvents))

    def exportICS(self, force=False, eventFilter=None):
        if not self._hasCrawledNewEvent and not force:
            return False

        cal = icalendar.Calendar()
        cal.add('prodid', '-//GT calendar 1.1//')
        cal.add('version', '2.0')

        exportEvents = sorted(filter(eventFilter, self.crawledEvents))
        for event in exportEvents:
            iEvent = event.toICAL(defaultEventLength=self.config.eventLength)
            cal.add_component(iEvent)
        return cal


class RemoteCalendarCrawler:
    IDENTIFIER = ''
    DOMAIN = ''

    def __init__(self, config, requester):
        self.config = config
        self.requester = requester

    def getEventDetails(self, eventURL):
        try:
            event = self._getEventDetails(eventURL)

            if isinstance(event, (set, frozenset, list, tuple)):
                for ev in event:
                    logger.debug(ev)
            elif event:
                logger.debug(event)
        except ValueError as e:
            logger.error('Cannot fetch information for %s%s',
                         base=self.DOMAIN, url=eventURL)
            logger.exception(e)
            return None

        return event

    def _getEventDetails(self, eventURL):
        raise Exception('Not implemented')


class RemoteListCrawler(RemoteCalendarCrawler):
    def getEventList(self):
        raise Exception('Not implemented')


class RemoteMonthCrawler(RemoteCalendarCrawler):
    def getMonthEventList(self, year, month):
        raise Exception('Not implemented')


class RemoteWeekCrawler(RemoteCalendarCrawler):
    def getWeekEventList(self, year, week):
        raise Exception('Not implemented')


class RemoteDayCrawler(RemoteCalendarCrawler):
    def getDayEventList(self, year, month, day):
        raise Exception('Not implemented')


class MercuryBackendCrawler(RemoteListCrawler):
    IDENTIFIER = 'hg.gatech.edu'
    DOMAIN = 'http://hg.gatech.edu'
    URL = '/views/ajax'

    # extracted from ajax calls
    LIST_REQUEST_DATA = {
        'type_1': 'event',
        'title': '',
        'view_name': 'content_tools',
        'view_display_id': 'block',
        'view_args': '',
        'pager_element': 0,
        'ajax_html_ids': [
            # this list is not strictly required by the server but we'll keep it not to break anything
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

    SELECTOR_EVENT_TITLE = '#meat #content #page-title'
    SELECTOR_EVENT_TIMES = '#meat #content .content .eventWrapper .detailsSidebar .allTimesList li'
    SELECTOR_EVENT_DETAILS = '#meat #content .content .eventWrapper .detailsSidebar'
    SELECTOR_EVENT_DESCRIPTION = '#meat #content .content .eventWrapper'
    SELECTOR_EVENT_DESCRIPTION_EXCLUDE = ('detailsSidebar', 'metaDataTitle', 'metaDataList', 'relatedLinksTitle', 'relatedLinksList')
    SELECTOR_EVENT_LINKS = '#meat #content .content .eventWrapper .relatedLinksList a'
    SELECTOR_EVENT_METADATA = '#meat #content .content .eventWrapper .metaDataList'

    FORMAT_POSTED = '%a, %b %d, %Y - %I:%M%p'
    FORMAT_EVENT_DATE = '%A %B %d, %Y'
    FORMAT_EVENT_TIME = '%I:%M %p'

    def getEventList(self):
        url = utils.normalizeURL(base=self.DOMAIN, url=self.URL)
        lastPosted = datetime.now()
        crawlUntil = lastPosted - self.config.mercurySearchBacklog
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

            if not soup.select(self.SELECTOR_NEXT_PAGE):
                break

            page+= 1

        return events

    def _getEventDetails(self, eventURL):
        details = self.requester.fetchURL(eventURL)
        if not details:
            return None

        def _descriptionFilter(elem):
            if not utils.Soup.filters.textElemFilter(elem):
                return False
            if isinstance(elem, str):
                return True
            classes = elem.attrs.get('class', ())
            return not any(cls in classes
                           for cls in self.SELECTOR_EVENT_DESCRIPTION_EXCLUDE)

        # HTML tree is broken sometimes, which will confuse the native python parser
        soup = BeautifulSoup(details, 'lxml')
        title = utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_TITLE)
        description, links = utils.Soup.tokenizeElemAt(soup,
                                                       self.SELECTOR_EVENT_DESCRIPTION,
                                                       elemFilter=_descriptionFilter,
                                                       base=eventURL)

        times = soup.select(self.SELECTOR_EVENT_TIMES)
        if not times:
            return None

        details = self._getBlockFromList(soup,
                                         self.SELECTOR_EVENT_DETAILS,
                                         'event details')
        if not details:
            logger.error('No event details found for {eventURL}')
            return None

        extras = self._getBlockDetailText(details, 'extras')
        location = self._getBlockDetailText(details, 'location')
        additionalURLs = self._getBlockDetail(details, 'url', onlyNextElem=False)
        if additionalURLs:
            links|= utils.Soup.getLinksAt(additionalURLs, 'a')
        newLinks = utils.Soup.getLinksAt(soup, self.SELECTOR_EVENT_LINKS)
        links|= set(filter(None, map(utils.normalizeURL(base=eventURL), newLinks)))

        audienceBlock = self._getBlockFromList(soup,
                                               self.SELECTOR_EVENT_METADATA,
                                               'invited audience')
        if not audienceBlock:
            logger.warning(f'Invalid HTML at {eventURL}')
        audience = utils.Soup.getElemText(audienceBlock)

        statusBlock = self._getBlockFromList(soup,
                                             self.SELECTOR_EVENT_METADATA,
                                             'status')
        if not statusBlock:
            logger.warning(f'Invalid HTML at {eventURL}')
            status = None
        else:
            status = self._getBlockDetailText(statusBlock, 'workflow status')
            status = status.lower()
            if status is not None and status not in self.config.statusCodes:
                logger.warning(f'Event {eventURL} has unknown status "{status}"!')

        if not self.config.statusCodes.get(status.lower(), 25):
            logger.info(f'Skipping {eventURL}: status {status}')
            return None

        events = set()
        for n, timeEntry in enumerate(times):
            startTime, endTime = self._parseEventTime(timeEntry)
            if not startTime:
                continue

            rawEvent = RawEvent(self.IDENTIFIER,
                                f'{eventURL}#{n}',
                                title,
                                startTime)
            rawEvent.setEnd(endTime)
            rawEvent.setExtras(extras)
            rawEvent.setLocation(location)
            rawEvent.setDescription(description)
            rawEvent.setLinks(links)
            rawEvent.setAudience(audience)
            rawEvent.setStatus(status)
            events.add(rawEvent)
        return events

    def _parseEventTime(self, timeEntry):
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

            return (utils.normalizeDate(sTime, self.config.timezone),
                        utils.normalizeDate(eTime, self.config.timezone))
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
            print('No blocklist', selector)
            return None

        def _isBlock(elem):
            return elem.text.strip().lower() == blockName

        detailsLabel = elems.findChild(_isBlock)
        if not detailsLabel:
            print('No label', blockName)
            return None

        return detailsLabel.findNextSibling()

    def _getBlockDetailText(self, detailSoup, detail):
        return utils.Soup.getElemText(self._getBlockDetail(detailSoup, detail))

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
        startTime = utils.normalizeDate(startTime, self.config.timezone)

        event = RawEvent(self.IDENTIFIER, eventURL, title, startTime)
        event.setEnd(utils.normalizeDate(endTime, self.config.timezone))
        event.setLocation(utils.Soup.getTextAt(soup, self.SELECTOR_EVENT_LOCATION))

        description, links = utils.Soup.tokenizeElemAt(soup,
                                                       self.SELECTOR_EVENT_DESCRIPTION,
                                                       base=eventURL)
        newLinks = utils.Soup.getLinksAt(soup, self.SELECTOR_EVENT_LINKS)
        links|= set(filter(None, map(utils.normalizeURL(base=eventURL), newLinks)))
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
        startTime = utils.normalizeDate(startTime, self.config.timezone)

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
        rawEvent.setDescription(description)
        rawEvent.setLinks(links)

        return rawEvent


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
        backlogTime = datetime.now() - self.config.backlog
        backlogTime = self.config.timezone.localize(backlogTime)
        reqData['endsAfter'] = backlogTime.replace(microsecond=0).isoformat()
        res = self.requester.fetchURL(url, data=reqData, json=True)

        events = set()
        for event in res['value']:
            if (event['status'] or '').lower() not in self.config.statusCodes:
                logger.warning(f'Event {eventURL} has unknown status "{event["status"]}"!')
            elif not self.config.statusCodes.get(event['status'].lower(), 25):
                continue

            eventURL = utils.normalizeURL(base=self.DOMAIN, url=self.EVENT_URL)
            eventURL = eventURL.format(event['id'])

            startTime = datetime.fromisoformat(event['startsOn'])
            startTime = utils.normalizeDate(startTime, self.config.timezone)

            rawEvent = RawEvent(self.IDENTIFIER,
                                eventURL,
                                event['name'],
                                startTime)

            if 'endsOn' in event:
                endTime = datetime.fromisoformat(event['endsOn'])
                endTime = utils.normalizeDate(endTime, self.config.timezone)
                rawEvent.setEnd(endTime)

            rawEvent.setExtras(', '.join(event.get('benefitNames', ())))

            soup = BeautifulSoup(event['description'], 'html.parser')
            description, links = utils.HTMLToText.tokenizeSoup(soup,
                                                               base=url,
                                                               customStyle=self.DESCRIPTION_STYLE)
            rawEvent.setDescription(description)
            rawEvent.setLinks(links)

            rawEvent.setStatus(event['status'])
            events.add(rawEvent)

        return events


def loadFromGit(token, config):
    icsDir, icsFileContent = os.path.split(config.icsFile)
    jsonDir, jsonFileContent = os.path.split(config.jsonFile)

    try:
        git = Github(token)
        repo = git.get_repo(config.repository)
        contents = repo.get_dir_contents('.')
        icsDir = repo.get_dir_contents(icsDir)
        if jsonDir == icsDir:
            jsonDir = icsDir
        else:
            jsonDir = repo.get_dir_contents(jsonDir)
    except GithubException as e:
        logger.error(f'Cannot open repository {config.repository}')
        logger.exception(e)
        return None, None, None, None

    icsSha = None
    jsonSha = None
    for file in icsDir:
        if file.name == config.icsFile:
            icsSha = file.sha
        elif file.name == config.jsonFile:
            jsonSha = file.sha

    if not icsSha:
        logger.warning(f'No previous ICS file {config.icsFile} present!')
    if not jsonSha:
        logger.warning(f'No previous JSON file {config.jsonFile} present!')
        return repo, icsSha, None, None

    jsonFileContent = None
    try:
        jsonFileContent = repo.get_git_blob(jsonSha)
        jsonFileContent = b64decode(jsonFileContent.content)
    except GithubException as e:
        logger.error(f'Cannot download previous JSON file {config.jsonFile}!')
        logger.exception(e)

    return repo, icsSha, jsonSha, jsonFileContent


def storeToGit(repo, config, icsSha, icsFileContent, jsonSha, jsonFileContent):
    try:
        if icsSha:
            repo.update_file(config.icsFile,
                             f'Update {config.icsFile}',
                             icsFileContent.to_ical(),
                             icsSha)
        else:
            repo.create_file(config.icsFile,
                             f'Initialize {config.icsFile}',
                             icsFileContent.to_ical())

        if jsonSha:
            repo.update_file(config.jsonFile,
                             f'Update {config.jsonFile}',
                             json.dumps(jsonFileContent, indent=2),
                             jsonSha)
        else:
            repo.create_file(config.jsonFile,
                             f'Initialize {config.jsonFile}',
                             json.dumps(jsonFileContent, indent=2))
    except GithubException as e:
        logger.error(f'Error: cannot push {config.icsFile} and {config.jsonFile} to repository:')
        logger.exception(e)
        return False
    return True


def main(token, args):
    config = CrawlerConfig.fromIni(args.config)
    rootLogger.setLevel(args.log or config.logLevel)

    RawEvent.BASE_TZ = config.timezone

    crawler = Crawler(config)
    crawler.registerCalendarCrawler(MercuryBackendCrawler)
    crawler.registerCalendarCrawler(CareerGatechEduCrawler)
    crawler.registerCalendarCrawler(CampuslabsComCrawler)
    crawler.registerCalendarCrawler(ChemistryGatechEduCrawler)

    repo, icsSha, jsonSha, jsonContent = loadFromGit(token, config)
    if not repo:
        return 1

    importEvents = json.loads(jsonContent or '[]')
    if importEvents and not args.ignore_previous_crawls:
        crawler.importJSON(importEvents)

    crawler.discover()
    crawler.resolve()

    exportedJSON = crawler.exportJSON(force=args.force_write)
    exportedICS = crawler.exportICS(force=args.force_write)

    if exportedJSON is False or exportedICS is False:
        logger.info('No new events')
        return 0

    res = storeToGit(repo, config, icsSha, exportedICS, jsonSha, exportedJSON)
    return 0 if res else 1


def parseArgs():
    parser = argparse.ArgumentParser(description='Crawls event across departments at Georgia Tech')
    parser.add_argument('--config', '-c', required=True, help='Path to the parser config file')
    parser.add_argument('--log', '-l', choices=('NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
                        default=None, help='Log level')
    parser.add_argument('--ignore-previous-crawls', '-i', default=False,
                        action='store_true', help='Ignore events from previous crawls')
    parser.add_argument('--force-write', '-f', default=False,
                        action='store_true', help='Force write to Github even if no new events were found')
    return parser.parse_args()


if __name__ == '__main__':
    token = os.environ['ACCESS_TOKEN']
    if not token:
        logger.error('No access token provided')
        exit(1)

    args = parseArgs()

    exit(main(token, args))
