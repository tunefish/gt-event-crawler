import calendar
import html
import logging
import math
import operator
import re
import time

from datetime import datetime, date
from urllib.parse import urlparse, urljoin

import icalendar
import requests

from bs4 import BeautifulSoup
from bs4.element import Comment as BS4Comment
from tabulate import tabulate


logger = logging.getLogger(__name__)


MEDIA_FILEEXT = (
    '.jpg',
    '.jpeg',
    '.png',
    '.gif',
    '.bmp',
    '.tiff',
    '.gifv',
    '.webm',
    '.webp',
)


def firstOrNone(l):
    return next(iter(l), None) if l else None


def intLen(i):
    # number of digits in a number (without sign)
    if i == 0:
        return 1
    else:
        return int(math.log10(abs(i)) + 1)


def normalizeDate(d, timezone=None):
    if isinstance(d, datetime) and timezone and not d.tzinfo:
        return timezone.localize(d)
    return d


def getDate(d):
    if isinstance(d, datetime):
        return d.date()
    elif isinstance(d, date):
        return d
    else:
        return None


def addMonths(d, months):
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def normalizeURL(base=None, url=None):
    def _normalize(url):
        try:
            parsed = urlparse(url)
        except ValueError:
            return None
        if parsed.scheme == 'mailto':
            return parsed.path.strip()
        return urljoin(base, url)
    if url is None:
        return _normalize
    return _normalize(url)


def isEmail(link):
    if not link:
        return False
    try:
        return urlparse(link).scheme == 'mailto'
    except ValueError:
        return False


def strfURL(urlFmt, year=None, month=None, week=None, day=None, page=0):
    dt = datetime.now()
    d = datetime(year=year or dt.year,
                 month=month or dt.month,
                 day=day or dt.day)
    return d.strftime(urlFmt.replace('%P', str(page)).replace('%V', str(week)))


def getLinksAtURL(requester, url, selector, **kwargs):
    page = requester.fetchURL(url)
    if not page:
        return set()

    soup = BeautifulSoup(page, 'html.parser')
    return set(filter(None, map(normalizeURL(base=url),
                                Soup.getLinksAt(soup, selector, **kwargs))))


def cleanText(text):
    return html.unescape(text.replace('\n', '')
                             .replace('\r', '')
                             .replace('\xa0', ' ')
                             .replace('  ', ' ')).strip()


def cleanTextFromIcal(text):
    return html.unescape(text.replace('\xa0', ' ')).strip()


def isSubsequenceByWord(subseq, string):
    def _stripNonAlnums(word):
        return ''.join(filter(str.isalnum, word))
    subseq = filter(None, map(_stripNonAlnums, subseq.split()))
    string = filter(None, map(_stripNonAlnums, string.split()))
    return all(word in string for word in subseq)


def mergeStringLists(mainList, mergeList):
    entries = list(map(str.strip, mainList.split(',')))
    entriesLower = list(map(str.lower, entries))
    for entry in map(str.strip, mergeList.split(',')):
        if entry.lower() not in entriesLower:
            entries.append(entry)
            entriesLower.append(entry.lower())
    return ', '.join(entries)


def tabulateWithHeaderFooter(data, headers, footer, **kwargs):
    table = tabulate((*data, footer), headers=headers, **kwargs)
    hline = table.split('\n', 2)[1]
    table, lastRow = table.rsplit('\n', 1)
    return f'{table}\n{hline}\n{lastRow}'


class Soup:
    class filters:
        @staticmethod
        def textElemFilter(elem):
            return (not isinstance(elem, BS4Comment) and
                    not (isinstance(elem, str) and elem == '\n'))

        @staticmethod
        def linkFilter(link):
            if not link:
                return False
            netlocAndPath = link.split('#')[0]
            return not any(netlocAndPath.lower().endswith(ext)
                           for ext in MEDIA_FILEEXT)

    @staticmethod
    def tokenizeElemAt(elem, selector, **kwargs):
        elem = firstOrNone(elem.select(selector))
        if not elem:
            return (), frozenset()
        return HTMLToText.tokenizeSoup(elem, **kwargs)

    @staticmethod
    def getTimeAt(elem, selector, attr):
        return Soup.getElemTime(firstOrNone(elem.select(selector)), attr)

    @staticmethod
    def getElemTime(elem, attr):
        if not elem or not elem.name or attr not in elem.attrs:
            return None
        return datetime.fromisoformat(elem.attrs[attr])

    @staticmethod
    def getLinkAt(elem, selector):
        return Soup.getElemLink(firstOrNone(elem.select(selector)))

    @staticmethod
    def getLinksAt(elem, selector, elemFilter=None, linkFilter=None):
        useFilter = linkFilter or Soup.filters.linkFilter
        elems = filter(elemFilter, elem.select(selector))
        elems = map(Soup.getElemLink, elems)
        return frozenset(filter(useFilter, elems))

    @staticmethod
    def getElemLink(elem):
        if not elem or not elem.name:
            return None
        return elem.attrs.get('href', None)

    @staticmethod
    def getTextAt(elem, selector):
        return Soup.getElemText(firstOrNone(elem.select(selector)))

    @staticmethod
    def getChildrenTexts(elem, elemFilter=None):
        useFilter = elemFilter or Soup.filters.textElemFilter
        if not elem:
            return None
        if not elem.name:
            return tuple(cleanText(elem))
        elems = filter(useFilter, elem.children)
        return tuple(filter(None, map(Soup.getElemText, elems)))

    @staticmethod
    def getElemText(elem):
        if not elem:
            return None
        if not elem.name:
            return cleanText(elem)
        return cleanText(elem.text)


class HTMLToText:
    # A placeholder for a link that will be replaced with a reference to the
    # corresponding entry in the link section during rendering.
    class ReplaceLink:
        __slots__ = ('link',)
        def __init__(self, link):
            self.link = link

        def __str__(self):
            return f'Link to {self.link}'

        def __repr__(self):
            return f'<{self.__str__()}>'

    # Indicates a weak line break in the token tuple.
    # During rendering, if a WeakLinebreak token does not neighbor a \n character,
    # a new \n character is inserted in its place. Otherwise it is just removed.
    class WeakLinebreak:
        __slots__ = ()

        def __str__(self):
            return f'<weak \\n>'

        def __repr__(self):
            return self.__str__()
    WeakLinebreak = WeakLinebreak()

    ELEM_FORMAT_PREPEND = {
        'h1': '\n\n',
        'h2': '\n\n',
        'h3': '\n\n',
        'h4': '\n',
        'h5': '\n',
        'h6': '\n',
        'ul': '\n',
        'li': '-',
    }

    ELEM_FORMAT_APPEND = {
        'h1': WeakLinebreak,
        'h2': WeakLinebreak,
        'h3': WeakLinebreak,
        'h4': WeakLinebreak,
        'h5': WeakLinebreak,
        'h6': WeakLinebreak,
        'li': WeakLinebreak,
        'ul': '\n\n',
        'p':  '\n\n',
    }

    ELEM_FORMAT_REPLACE = {
        'br': '\n',
    }

    @staticmethod
    def _textMatchesLink(link, nestedTokens, base=None):
        link = Soup.getElemLink(link) or ''
        linkIsEmail = isEmail(link)
        link = normalizeURL(base=base or '', url=link)

        if not link:
            return False

        linkParsed = urlparse(link)

        if nestedTokens and len(nestedTokens) == 1:
            try:
                strParsed = urlparse(nestedTokens[0])
            except:
                return False
            simpleLinkUrl = linkParsed.netloc + linkParsed.path
            perfMatch = (strParsed.netloc == linkParsed.netloc
                         and strParsed.path == linkParsed.path)
            simpleMatch =  (not strParsed.netloc
                            and simpleLinkUrl.startswith(strParsed.path)
                            and simpleLinkUrl[len(strParsed.path):] in ('', '/'))
            restMatch = (strParsed.query == linkParsed.query
                         and strParsed.fragment == linkParsed.fragment)
            return (perfMatch or simpleMatch) and restMatch and not linkIsEmail
        return False

    @staticmethod
    def tokenizeSoup(elem,
                     elemFilter=None,
                     linkFilter=None,
                     base=None,
                     customStyle=None):
        tokens = []
        links = set()
        elemFilter = elemFilter or Soup.filters.textElemFilter
        linkFilter = linkFilter or Soup.filters.linkFilter

        customStyle = customStyle or {}
        customStyle['FORMAT_REPLACE'] = customStyle.get('FORMAT_REPLACE', {})
        customStyle['FORMAT_PREPEND'] = customStyle.get('FORMAT_PREPEND', {})
        customStyle['FORMAT_APPEND'] = customStyle.get('FORMAT_APPEND', {})

        for child in elem:
            if not elemFilter(child):
                continue

            tag = child.name
            if isinstance(child, str):
                tokens.append(cleanText(child))
            elif customStyle['FORMAT_REPLACE'].get(tag, None) is not None:
                tokens.append(customStyle['FORMAT_REPLACE'][tag])
            elif child.name in HTMLToText.ELEM_FORMAT_REPLACE:
                tokens.append(HTMLToText.ELEM_FORMAT_REPLACE[tag])
            else:
                nestedTkns, nestedLnks = HTMLToText.tokenizeSoup(child,
                                                                 elemFilter=elemFilter,
                                                                 linkFilter=linkFilter,
                                                                 base=base,
                                                                 customStyle=customStyle)
                links|= nestedLnks

                if child.name == 'a' and linkFilter(Soup.getElemLink(child)):
                    # only print the link text of links whose text is not equal to
                    # (or a human readable approximation of, e.g. ignoring the
                    # scheme) the link's text
                    if not HTMLToText._textMatchesLink(child, nestedTkns, base=base):
                        tokens.extend(nestedTkns)

                    link = Soup.getElemLink(child)
                    tokens.append(HTMLToText.ReplaceLink(link))
                    links.add(link)
                else:
                    # preformatting
                    if customStyle['FORMAT_PREPEND'].get(tag, None) is not None:
                        tokens.append(customStyle['FORMAT_PREPEND'][tag])
                    elif child.name in HTMLToText.ELEM_FORMAT_PREPEND:
                        tokens.append(HTMLToText.ELEM_FORMAT_PREPEND[tag])

                    tokens.extend(nestedTkns)

                    # postformatting
                    if customStyle['FORMAT_APPEND'].get(tag, None) is not None:
                        tokens.append(customStyle['FORMAT_APPEND'][tag])
                    elif child.name in HTMLToText.ELEM_FORMAT_APPEND:
                        tokens.append(HTMLToText.ELEM_FORMAT_APPEND[tag])

        return tuple(filter(None, tokens)), frozenset(links)

    @staticmethod
    def renderTokens(tokens, links, specialLinks=None):
        # Step 0: Init link map and determine required number of digits
        linkMap = {l: -1 for l in links}
        for n, link in enumerate(specialLinks or ()):
            linkMap[link] = n
        counter = len(specialLinks or ()) or 1

        linkMap.update({l.link: -1
                       for l in tokens if isinstance(l, HTMLToText.ReplaceLink)})
        digits = intLen(len(linkMap))

        # Step 1a: insert \n at WeakLinebreak tokens where necessary and replace
        #          ReplaceLink tokens with a referene to the link
        fragments = ['']
        for d in tokens:
            if isinstance(d, str):
                if fragments[-1] is HTMLToText.WeakLinebreak:
                    fragments.pop()
                    if not d.startswith('\n'):
                        fragments.append(f'\n{d}')
                        continue
                fragments.append(d)
            elif d is HTMLToText.WeakLinebreak:
                # Ignore double WeakLinebreaks
                if (fragments[-1] is not HTMLToText.WeakLinebreak
                    and not fragments[-1].endswith('\n')):
                    fragments.append(HTMLToText.WeakLinebreak)
            elif isinstance(d, HTMLToText.ReplaceLink):
                if linkMap[d.link] == -1:
                    linkMap[d.link] = counter
                    counter+= 1
                if fragments[-1] is HTMLToText.WeakLinebreak:
                    fragments.pop()
                fragments.append(f'[{linkMap[d.link]:0{digits}}]')

        if fragments[-1] is HTMLToText.WeakLinebreak:
            fragments.pop()

        # Step 1b: join the fragments to get a continuous piece of text, removing
        #          triple newlines and double, leading and trailing spaces
        fragments = ' '.join(fragments).strip()
        fragments = re.sub(r' {2,}', ' ', fragments)
        fragments = re.sub(r' ([\n.,;:!?])', r'\1', fragments)
        fragments = re.sub(r'\n ', '\n', fragments)
        renderedText = re.sub(r'\n{3,}', '\n\n', fragments)

        # Step 2a: assign IDs to links not referenced by a ReplaceLink token
        for link, ctr in linkMap.items():
            if ctr == -1:
                linkMap[link] = counter
                counter+= 1

        # Step 2b: render links section
        sortedLinks = sorted(linkMap.items(), key=operator.itemgetter(1))
        linkDescription = (f'[{i:0{digits}}]  {link}' for link, i in sortedLinks)
        renderedLinks = '\n'.join(linkDescription)

        return renderedText, renderedLinks


class Requester:
    __slots__ = ('userAgent', 'retries', 'retryTimeout',
                 'buckets', 'maxTokens', 'tokenRate')

    def __init__(self, userAgent=None, retries=None, retryTimeout=None,
                 maxTokens=10000, tokenRate=10000):
        self.userAgent = userAgent
        self.retries = retries
        self.retryTimeout = retryTimeout
        self.buckets = {}
        self.maxTokens = maxTokens
        self.tokenRate = tokenRate

    def setUseragent(self, useragent):
        self.useragent = useragent

    def setRetries(self, retries):
        self.retries = retries

    def setRetryTimeout(self, retryTimeout):
        self.retryTimeout = retryTimeout

    def setMaxTokens(self, maxTokens):
        self.maxTokens = maxTokens

    def setTokenRate(self, tokenRate):
        self.tokenRate = tokenRate

    def fetchURL(self, url,
                 base=None,
                 method='GET',
                 data=None,
                 retries=None,
                 json=False):
        if base:
            url = normalizeURL(base=base, url=url)

        self._throttle(url)

        for n in range(self.retries if retries is None else retries):
            try:
                response = requests.request(method,
                                            url,
                                            headers={"User-Agent": self.userAgent},
                                            data=data)
            except Exception as e:
                logger.error(f'{url}: exception (attempt {n})')
                logger.exception(e)
                continue

            if response.status_code != 200:
                logger.error(f'{url}: status code {response.status_code} (attempt {n})\n{response.headers}\n{response.text:1000}')
                continue

            if n:
                logger.info(f'{url} fetched (attempts {n})')

            if json:
                return response.json()
            else:
                return response.text

        return None

    def fetchICalEvents(self, url, base=None):
        if not url:
            return []

        ical = self.fetchURL(url, base=base)
        if not ical:
            return []

        try:
            cal = icalendar.Calendar.from_ical(ical)
        except ValueError:
            return []

        return cal.walk('VEVENT')

    def _throttle(self, url):
        host = urlparse(url).netloc.lower()
        if host.startswith('www.'):
            host = host[4:]

        now = time.monotonic()

        # load state
        if host not in self.buckets:
            tokens = self.maxTokens
            lastToken = now
        else:
            tokens, lastToken = self.buckets[host]

        # refill bucket
        if tokens < self.maxTokens:
            newTokens = (now - lastToken) * self.tokenRate
            tokens = min(self.maxTokens, tokens + newTokens)
        lastToken = now

        # wait for it to fill
        if tokens >= 1:
            tokens-= 1
        else:
            wait = (1 - tokens)/self.tokenRate
            wait = math.ceil(wait*1000)/1000
            time.sleep(wait)

            tokens = 0
            lastToken = time.monotonic()

        # store state
        self.buckets[host] = (tokens, lastToken)
