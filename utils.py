import calendar
import html
import importlib
import logging
import math
import operator
import re

from datetime import datetime, time, date
from time import monotonic as time_monotonic
from time import sleep as time_sleep
from urllib.parse import urlparse, urlunparse, urljoin

import icalendar
import pytz
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


def itemsFromMapping(mapping):
    return mapping.items() if hasattr(mapping, 'items') else mapping


def updateDictPath(dictionary, path, value, sep='.'):
    *path, entryName = path.split(sep)

    current = dictionary
    for name in path:
        entry = current.get(name, {})
        current[name] = entry
        current = entry

    current[entryName] = value
    return dictionary


def importClass(name, baseModule=None):
    path = name.split('.')
    if baseModule:
        path[0] = baseModule + path[0]

    module = importlib.import_module(path[0])

    # Find the requested class in the module/class tree
    if path:
        elem = module
        for pName in path[1:]:
            nextElem = getattr(elem, pName, None)
            if nextElem is None:
                raise ModuleNotFoundError(f'Cannot find {pName} of {elem} in path {name}')
            elem = nextElem
        clazz = elem
    else:
        clazz = module
    return clazz


def intLen(i):
    # number of digits in a number (without sign)
    if i == 0:
        return 1
    else:
        return int(math.log10(abs(i)) + 1)


def normalizeDate(d, timezone=None):
    if isinstance(d, (datetime, time)):
        if not d.tzinfo and timezone:
            d = timezone.localize(d)
        return d.astimezone(pytz.utc)
    return d


def getDate(d):
    if isinstance(d, datetime):
        return d.date()
    elif isinstance(d, date):
        return d
    else:
        return None


def getDatetime(d):
    if isinstance(d, datetime):
        return d
    elif isinstance(d, date):
        return datetime(*d.timetuple()[:6])
    else:
        return None


def addMonths(d, months):
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def parseIsoDateOrDatetime(iso):
    try:
        return date.fromisoformat(iso)
    except ValueError:
        pass

    try:
        return time.fromisoformat(iso)
    except ValueError:
        pass

    try:
        return datetime.fromisoformat(iso)
    except ValueError:
        return None


_DOMAIN_REGEX = re.compile(r'([\w\d]([\w\d-]{0,61}[\w\d])?\.)+[\w\d]+')
def normalizeURL(base=None, url=None, fixNetloc=True):
    def _normalize(url):
        if isEmail(url):
            try:
                parsed = urlparse(url)
            except ValueError:
                return url
            return parsed.path.strip().lower()

        # Some URLs are missing the scheme and would be normalized to a URL pointing
        # somewhere on the crawled calendar's domain
        #  -> if the part of a URL before the first forward slash looks like a
        #     domain name, add 'http' as scheme
        try:
            parsed = urlparse(url)
        except ValueError:
            return None
        scheme, netloc, path, *_ = parsed
        if not scheme:
            if not netloc and fixNetloc:
                firstPathElem, *pathRest = path.split('/', 1)
                if (0 < len(firstPathElem) <= 253
                    and _DOMAIN_REGEX.fullmatch(firstPathElem)):
                    scheme = 'http'
                    netloc = firstPathElem
                    path = (pathRest or ('',))[0]
            else:
                scheme = 'http'
        # remove unneccessary characters (eg bogus '#') and use lowercase scheme + netloc
        url = urlunparse((scheme.lower(),
                          netloc.lower(),
                          path,
                          *parsed[3:]))
        return urljoin(base, url)
    if url is None:
        return _normalize
    return _normalize(url)


_EMAIL_REGEX = re.compile(r'[^\s:;/@]+@[^\s:;/@]+\.[^\s:;/@]+')
def isEmail(link):
    if not link:
        return False
    try:
        if urlparse(link).scheme == 'mailto':
            return True
    except ValueError:
        pass

    return bool(_EMAIL_REGEX.fullmatch(link))


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


def sortStringList(strList):
    if strList is None:
        return None
    return ', '.join(sorted(map(str.strip, strList.split(','))))


def mergeStringLists(mainList, mergeList):
    if not mainList or not mergeList:
        return mainList or mergeList

    entries = list(filter(None, map(str.strip, mainList.split(','))))
    entriesLower = list(map(str.lower, entries))
    for entry in filter(None, map(str.strip, mergeList.split(','))):
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
    def getDatetimeAt(elem, selector, attr):
        return Soup.getElemDatetime(firstOrNone(elem.select(selector)), attr)

    @staticmethod
    def getElemDatetime(elem, attr):
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
    def getTextAt(elem, selector, **kwargs):
        return Soup.getElemText(firstOrNone(elem.select(selector)), **kwargs)

    @staticmethod
    def getTextsAt(elem, selector, elemFilter=None):
        elemFilter = elemFilter or Soup.filters.textElemFilter
        elems = filter(elemFilter, elem.select(selector))
        return tuple(filter(None, map(Soup.getElemText, elems)))

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


class NormalizedURLContainer:
    _fix_netloc = False
    # A container superclass which normalizes URLs and prefers their HTTPS 
    # All subclasses must list this class BEFORE the container type they are extending
    def __contains__(self, key):
        normKey = self._normalizeURL(key)
        if super().__contains__(normKey):
            return True
        parsed = urlparse(normKey)
        if parsed.scheme == 'https':
            httpURL = urlunparse(('http', *parsed[1:]))
            return super().__contains__(httpURL)
        return False

    def _normalizeURL(self, url):
        url = normalizeURL(url=url, fixNetloc=self._fix_netloc)
        parsed = urlparse(url)
        if parsed.scheme == 'http':
            httpsURL = urlunparse(('https', *parsed[1:]))
            if super().__contains__(httpsURL):
                return httpsURL
        return url


class NormalizedURLSet(NormalizedURLContainer, set):
    def __init__(self, *args, fixNetloc=True):
        args = ((map(self._normalizeURL, arg)) for arg in args)
        self._fix_netloc = fixNetloc
        super(NormalizedURLSet, self).__init__(*args)

    def add(self, url):
        normURL = self._normalizeURL(url)
        super().add(normURL)
        parsed = urlparse(normURL)
        if parsed.scheme == 'http':
            httpURL = urlunparse(('http', *parsed[1:]))
            if set.__contains__(self, httpURL):
                super().__delitem__(httpURL)

    def isdisjoint(self, other):
        return not any(url in self for url in other)

    def issubset(self, other):
        return self.__le__(other)

    def __le__(self, other):
        return all(url in other for url in self)

    def __lt__(self, other):
        return self.__le__(other) and self != other

    def issuperset(self, other):
        return self.__ge__(other)

    def __ge__(self, other):
        return all(url in self for url in other)

    def __gt__(self, other):
        return self.__ge__(other) and self != other

    def union(self, *others):
        return type(self)(super().union(*others))

    def __or__(self, other):
        return self.union(other)

    def intersection(self, *others):
        return type(self)(super().intersection(*others))

    def __and__(self, other):
        return self.intersection(other)

    def difference(self, *others):
        return type(self)(super().difference(*others))

    def __sub__(self, other):
        return self.difference(other)

    def symmetric_difference(self, other):
        return type(self)(super().__xor__(other))

    def __xor__(self, other):
        return self.symmetric_difference(other)

    def copy(self):
        return type(self)(super().copy())

    def update(self, *others):
        for other in others:
            for url in other:
                self.add(url)

    def remove(self, url):
        super().remove(self._normalizeURL(url))

    def discard(self, url):
        super().discard(self._normalizeURL(url))


class NormalizedURLDict(NormalizedURLContainer, dict):
    def __init__(self, *args, fixNetloc=True, **kwargs):
        args = (((self._normalizeURL(key), value)
                 for key, value in itemsFromMapping(arg))
                for arg in args)
        kwargs = {self._normalizeURL(key): value for key, value in kwargs}
        self._fix_netloc = fixNetloc
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        normKey = self._normalizeURL(key)
        super().__setitem__(normKey, value)
        parsed = urlparse(normKey)
        if parsed.scheme == 'https':
            httpURL = urlunparse(('http', *parsed[1:]))
            if dict.__contains__(self, httpURL):
                super(NormalizedURLDict, self).__delitem__(httpURL)

    def __getitem__(self, key):
        return super().__getitem__(self._normalizeURL(key))

    def __delitem__(self, key):
        super().__delitem__(self._normalizeURL(key))

    def get(self, key, default=None):
        return super().get(self._normalizeURL(key), default)

    def setdefault(self, key, value=None):
        return super().setdefault(self._normalizeURL(key), value)

    def pop(self, key, default=None):
        return super().pop(self._normalizeURL(key), default)

    def popitem(self):
        return super().popitem()

    def has_key(self, key):
        return dict.__contains__(self, self._normalizeURL(key))

    def update(self, *args, **kwargs):
        mappings = list(args) + [kwargs]
        for mapping in mappings:
            for key, value in itemsFromMapping(mapping):
                self[key] = value

    def copy(self):
        return type(self)(super().copy())

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, dict(self))


class HTMLToText:
    # A placeholder for a link that will be replaced with a reference to the
    # corresponding entry in the link section during rendering.
    class ReplaceLink:
        __slots__ = ('link',)
        def __init__(self, link):
            self.link = link

        def __eq__(self, other):
            return (isinstance(other, HTMLToText.ReplaceLink)
                    and other.link == self.link)

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

                    normLink = normalizeURL(base=base, url=Soup.getElemLink(child))
                    tokens.append(HTMLToText.ReplaceLink(normLink))
                    links.add(normLink)
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
    def stringifyTokens(tokens):
        def _stringify(tkn):
            if isinstance(tkn, str):
                if tkn.startswith('\\'):
                    return f'/{tkn}'
                else:
                    return tkn
            elif tkn is HTMLToText.WeakLinebreak:
                return '\\wn'
            elif isinstance(tkn, HTMLToText.ReplaceLink):
                return f'\\l{tkn.link}'

        return tuple(map(_stringify, tokens))

    @staticmethod
    def unstringifyTokens(stringifiedTokens):
        def _unstringify(tkn):
            if tkn.startswith('\\wn'):
                return HTMLToText.WeakLinebreak
            elif tkn.startswith('\\l'):
                return HTMLToText.ReplaceLink(tkn[2:])

            if tkn.startswith('\\'):
                return tkn[1:]
            else:
                return tkn

        return tuple(map(_unstringify, stringifiedTokens))

    @staticmethod
    def renderCleanTokens(tokens):
        textFltr = lambda x: x is HTMLToText.WeakLinebreak or isinstance(x, str)
        return HTMLToText.renderTokens(tuple(filter(textFltr, tokens)), ())[0]

    @staticmethod
    def renderTokens(tokens, links, specialLinks=None):
        # Step 0: Init link map and determine required number of digits
        linkMap = NormalizedURLDict({link: -1 for link in links})
        for tkn in tokens:
            if isinstance(tkn, HTMLToText.ReplaceLink):
                linkMap[tkn.link] = -1
        for n, link in enumerate(specialLinks or ()):
            linkMap[link] = n

        counter = len(specialLinks) if specialLinks else 1
        digits = intLen(len(linkMap) - 1)

        # Step 1a: insert \n at WeakLinebreak tokens where necessary and replace
        #          ReplaceLink tokens with a referene to the link
        fragments = ['']
        for tkn in tokens:
            if isinstance(tkn, str):
                if fragments[-1] is HTMLToText.WeakLinebreak:
                    fragments.pop()
                    if not tkn.startswith('\n'):
                        fragments.append(f'\n{tkn}')
                        continue
                fragments.append(tkn)
            elif tkn is HTMLToText.WeakLinebreak:
                # Ignore double WeakLinebreaks
                if (fragments[-1] is not HTMLToText.WeakLinebreak
                    and not fragments[-1].endswith('\n')):
                    fragments.append(HTMLToText.WeakLinebreak)
            elif isinstance(tkn, HTMLToText.ReplaceLink):
                if linkMap[tkn.link] == -1:
                    linkMap[tkn.link] = counter
                    counter+= 1
                if fragments[-1] is HTMLToText.WeakLinebreak:
                    fragments.pop()
                fragments.append(f'[{linkMap[tkn.link]:0{digits}}]')

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
        unassigned = filter(lambda l: l[1] == -1, linkMap.items())
        sorter = lambda link: (isEmail(link[0]), link[0])
        unassigned = sorted(unassigned, key=sorter)
        for ctr, (link, _) in enumerate(unassigned, start=counter):
            linkMap[link] = ctr

        # Step 2b: render links section
        sortedLinks = sorted(linkMap.items(), key=operator.itemgetter(1))
        linkDescription = (f'[{i:0{digits}}]  {link}'
                           for link, i in sortedLinks)
        renderedLinks = '\n'.join(linkDescription)

        return renderedText, renderedLinks


class Requester:
    __slots__ = ('session', 'userAgent', 'retries', 'retryTimeout',
                 'buckets', 'maxTokens', 'tokenRate')

    def __init__(self, userAgent=None, retries=None, retryTimeout=None,
                 maxTokens=10000, tokenRate=10000):
        self.session = requests.Session()

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
                 json=False,
                 errorOnCode=None):
        if base:
            url = normalizeURL(base=base, url=url)

        errorCodes = set((404,))
        if errorOnCode:
            errorCodes|= set(errorOnCode)
        self._throttle(url)

        for n in range((self.retries if retries is None else retries) or 1):
            try:
                # TODO: use urllib3's Retry
                response = self.session.request(method,
                                                url,
                                                headers={"User-Agent": self.userAgent},
                                                data=data)
            except Exception as e:
                logger.error(f'{url}: exception (attempt {n})')
                logger.exception(e)
                continue

            if response.status_code != 200:
                if response.status_code in errorCodes:
                    return None
                if not (500 <= response.status_code < 600):
                    resShort = response.text.replace('\n', '')[:1000]
                    logger.error(f'{url}: status code {response.status_code} (attempt {n})\n{response.headers}\n{resShort}')
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

        now = time_monotonic()

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
            time_sleep(wait)

            tokens = 0
            lastToken = time_monotonic()

        # store state
        self.buckets[host] = (tokens, lastToken)
