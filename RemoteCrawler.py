import logging


logger = logging.getLogger(__name__)


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
            logger.error(f'Cannot fetch information for {eventURL}')
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
