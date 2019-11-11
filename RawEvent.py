import logging

import pytz
import icalendar

from collections import defaultdict
from itertools import chain

from datetime import datetime, timedelta

import utils


logger = logging.getLogger(__name__)


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
        self.links = links or utils.NormalizedURLSet()
        self.audience = audience
        self.status = status

    def setAlternativeUrls(self, urls):
        self.alternativeUrls = utils.NormalizedURLSet(urls)

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
        self.links = utils.NormalizedURLSet(links)

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
        else:
            return utils.HTMLToText.renderCleanTokens(self.description)

    def merge(self, other):
        result = RawEvent(self.crawler,
                          self.url,
                          self.title,
                          self.start,
                          end=self.end)
        altUrls = ((self.alternativeUrls or set())
                   | set((other.url,))
                   | (other.alternativeUrls or set()))
        result.setAlternativeUrls(altUrls)

        if (not self.location
            or (other.location
                and self.location.lower() in other.location.lower())):
            # use other location other.location is more specific
            result.setLocation(other.location)
        else:
            result.setLocation(self.location)

        if not self.description:
            result.setDescription(other.description)
        elif other.description:
            # get rid of formatting, we only care about the unformatted text
            desc = self.getDescription()
            otherDesc = other.getDescription()
            if utils.isSubsequenceByWord(desc, otherDesc):
                result.setDescription(other.description)
            elif not utils.isSubsequenceByWord(otherDesc, desc):
                logger.warning(f'The event {self} from {self.url} and {other.url} have different descriptions!')
                result.setDescription((*self.description,
                                      '\n\n',
                                      *other.description))
            else:
                result.setDescription(self.description)
        else:
            result.setDescription(self.description)

        result.setLinks(self.links | (other.links or set()))
        result.setExtras(utils.mergeStringLists(self.extras, other.extras))
        result.setAudience(utils.mergeStringLists(self.audience, other.audience))

        if not self.status:
            result.setStatus(other.status)
        else:
            result.setStatus(self.status)

        return result

    def toICAL(self, defaultEventLength=None):
        event = icalendar.Event()
        event['UID'] = self.url
        event['SUMMARY'] = self.title
        event.add('DTSTART', self.start)
        if ('TZID' not in event['DTSTART'].params
            and isinstance(self.start, datetime)):
            tzid = icalendar.parser.tzid_from_dt(self.start)
            event['DTSTART'].params['TZID'] = tzid
        end = self.getEnd(delta=defaultEventLength)
        event.add('DTEND', end)
        if 'TZID' not in event['DTEND'].params and isinstance(end, datetime):
            tzid = icalendar.parser.tzid_from_dt(end)
            event['DTEND'].params['TZID'] = tzid
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
            event.setLinks(jObj['links'])
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
                    event = event.merge(mergeEvent)
                yield event

    def iterUniqueEvents(self):
        return iter(self)

    def iterAllEvents(self):
        return chain(*self.definingTuples.values())
