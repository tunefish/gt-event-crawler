import json
import logging
import operator

import icalendar
import pytz
import yaml

from collections import defaultdict
from datetime import datetime, timedelta
from time import monotonic as time_monotonic

import utils
from RawEvent import RawEvent, DedupEventSet
from RemoteCrawler import *


logger = logging.getLogger(__name__)


class Crawler:
    def __init__(self, config):
        self.config = config
        self.crawlers = list()
        self.events = defaultdict(set)

        self._processConfig()

        self.requester = utils.Requester(self.config['general']['user_agent'],
                                         self.config['crawler']['retries'],
                                         self.config['crawler']['retry_timeout'],
                                         self.config['throttling']['max_tokens'],
                                         self.config['throttling']['token_rate'])
        
        self._setupCrawlers()
        
        weights = {crawler.IDENTIFIER: crawler.config['weight']
                   for crawler in self.crawlers}
        self.crawledEvents = DedupEventSet(statusCodes=config['status_codes'],
                                           crawlerWeights=weights)
        self._hasCrawledNewEvent = False

    def _processConfig(self):
        # convert to required datatypes and set meaningful default values
        config = self.config
        general = config['general']
        general['log_level'] = general.get('log_level', 'INFO').upper()

        crawler = config['crawler']
        crawler['retries'] = crawler.get('retries', 0)
        crawler['retry_timeout'] = crawler.get('retry_timeout', 0)
        crawler['backlog'] = timedelta(days=crawler.get('backlog', 30))
        
        defs = crawler.get('defaults', {})
        defs['timezone'] = pytz.timezone(defs.get('timezone', 'utc'))
        defs['event_length'] = timedelta(minutes=defs.get('event_length', 60))
        crawler['defaults'] = defs

    def _setupCrawlers(self):
        for crawlerPath, config in self.config['crawlers'].items():
            try:
                # Try relative import from crawlers sub-module.
                # If that fails, import relative to root
                try:
                    clazz = utils.importClass(crawlerPath, baseModule='crawlers.')
                except ModuleNotFoundError:
                    clazz = utils.importClass(crawlerPath)

                # Sanity check
                if (not isinstance(clazz, type)
                    or not issubclass(clazz, RemoteCalendarCrawler)):
                    raise ValueError('Crawler must be a subclass of RemoteCalendarCrawler')

                crawlerConfig = self.config['crawler'].copy()
                crawlerConfig.update(config)
                self.crawlers.append(clazz(crawlerConfig, self.requester))
            except Exception as e:
                logger.error(f'Unable to load crawler {crawlerPath}:')
                logger.exception(e)

    def discover(self, skipExceptions=3):
        logger.info('Start fetching events for each calendar')
        stats = {cal: 0 for cal in self.crawlers}

        def _handleError(crawler, e):
            logger.error(f'An error occurred while trying to obtain a list of events for calendar {crawler.IDENTIFIER}')
            logger.exception(e)

        t_start = time_monotonic()
        for crawler in self.crawlers:
            exceptionCount = 0
            calDate = datetime.now()
            events = None
            t_crawler = time_monotonic()

            if isinstance(crawler, RemoteListCrawler):
                try:
                    self.events[crawler]|= crawler.getEventList()
                except Exception as e:
                  _handleError(crawler, e)
            elif isinstance(crawler, RemoteMonthCrawler):
                while exceptionCount < skipExceptions:
                    try:
                        events = crawler.getMonthEventList(year=calDate.year,
                                                            month=calDate.month)
                        logger.debug(f'Found {len(events)} events this round for {crawler.IDENTIFIER}')
                    except Exception as e:
                        _handleError(crawler, e)
                        exceptionCount+= 1
                    if not events:
                        break
                    self.events[crawler]|= events
                    calDate = utils.addMonths(calDate, 1)
            elif isinstance(crawler, RemoteWeekCrawler):
                while exceptionCount < skipExceptions:
                    try:
                        week = calDate.isocrawler()[1]
                        events = crawler.getWeekEventList(year=calDate.year,
                                                           week=week)
                        logger.debug(f'Found {len(events)} events this round for {crawler.IDENTIFIER}')
                    except Exception as e:
                        _handleError(crawler, e)
                        exceptionCount+= 1
                    if not events:
                        break
                    self.events[crawler]|= events
                    calDate = calDate + timedelta(days=7)
            elif isinstance(crawler, RemoteDayCrawler):
                while exceptionCount < skipExceptions:
                    try:
                        events = crawler.getDayEventList(year=calDate.year,
                                                          month=calDate.month,
                                                          day=calDate.day)
                        logger.debug(f'Found {len(events)} events this round for {crawler.IDENTIFIER}')
                    except Exception as e:
                        _handleError(crawler, e)
                        exceptionCount+= 1
                    if not events:
                        break
                    self.events[crawler]|= events
                    calDate += timedelta(days=1)
            else:
                logger.error(f'Unknown calendar type {type(crawler)}')
                continue

            stats[crawler]+= time_monotonic() - t_crawler
            logger.debug(f'Found {len(self.events[crawler])} events in total for {crawler.IDENTIFIER}')

        t_end = time_monotonic()
        logger.info(f'Event discovery took {t_end - t_start}s')

        tableData = tuple((cal.IDENTIFIER, len(self.events[cal]), stats[cal])
                          for cal in self.crawlers)
        headers = ('Crawler', 'Events', 'Processing Time (s)')
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
                 for cal in self.crawlers}

        logger.info('Start fetching event details')
        hasEvents = True
        while hasEvents:
            hasEvents = False
            for crawler in self.crawlers:
                t_crawler = time_monotonic()
                if self.events[crawler]:
                    hasEvents = True
                    ev = self.events[crawler].pop()
                    if ev is None:
                        loggger.warning(f'{crawler.IDENTIFIER} got None event!')
                        continue

                    try:
                        if not isinstance(ev, RawEvent):
                            ev = crawler.getEventDetails(ev)
                        if ev is not None:
                            if isinstance(ev, RawEvent):
                                self._addCrawledEvent(ev)
                                stats[crawler]['success']+= 1
                            elif isinstance(ev, (set, frozenset, tuple, list)):
                                for event in ev:
                                    self._addCrawledEvent(event)
                                stats[crawler]['success']+= 1 if ev else 0
                        else:
                            stats[crawler]['fail']+= 1
                    except Exception as e:
                        logger.error(f'An error occurred while trying to obtain the details of event {ev}')
                        logger.exception(e)
                        stats[crawler]['fail']+= 1
                stats[crawler]['time']+= time_monotonic() - t_crawler

        t_end = time_monotonic()
        logger.info(f'Event resolution took {t_end - t_start}s')

        headers = ('Crawler', 'Failure', 'Success', 'Total', 'Processing Time (s)')
        footers = ('Total',
                   sum(map(operator.itemgetter('fail'), stats.values())),
                   sum(map(operator.itemgetter('success'), stats.values())),
                   sum(map(operator.itemgetter('events'), stats.values())),
                   sum(map(operator.itemgetter('time'), stats.values())))
        tableData = tuple((cal.IDENTIFIER, *stats[cal].values())
                          for cal in self.crawlers)
        logger.info('Statistics:\n' + utils.tabulateWithHeaderFooter(tableData,
                                                                     headers,
                                                                     footers))

    def _addCrawledEvent(self, event):
        if event.status and event.status not in self.config['status_codes']:
            logger.warning(f'Event at {event.url} has unknown status "{event.status}"!')
        if datetime.now() - event.refDate() > self.config['crawler']['backlog']:
            # don't add events we don't care about anyways
            return

        if not self.config['status_codes'].get((event.status or '').lower(), 25):
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
            if now - event.refDate() <= self.config['crawler']['backlog']:
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
        defaultEventLength = self.config['crawler']['defaults']['event_length']
        for event in exportEvents:
            iEvent = event.toICAL(defaultEventLength=defaultEventLength)
            cal.add_component(iEvent)
        return cal
