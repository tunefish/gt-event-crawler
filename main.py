#!/usr/bin/env python3
import argparse
import json
import logging
import os
import yaml

from base64 import b64decode

from github import Github, GithubException
from github import UnknownObjectException as UnknownGithubObjectException

import utils
from Crawler import Crawler
from RawEvent import RawEvent


logging.basicConfig()
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

logger = logging.getLogger(__name__)


class OptionOverrideAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        if '=' not in value:
            raise ValueError('Option must be in format KEY=VALUE')
        key, val = value.split('=', 1)
        optionsDict = getattr(namespace, self.dest)
        if optionsDict:
            optionsDict[key] = val
        else:
            setattr(namespace, self.dest, {key: val})


def loadFromGit(token, config):
    icsDir, icsFileContent = os.path.split(config['ics_file'])
    jsonDir, jsonFileContent = os.path.split(config['json_file'])

    try:
        git = Github(token)
        repo = git.get_repo(config['repository'])
        contents = repo.get_dir_contents('.')
        icsDir = repo.get_dir_contents(icsDir)
        if jsonDir == icsDir:
            jsonDir = icsDir
        else:
            jsonDir = repo.get_dir_contents(jsonDir)
    except GithubException as e:
        logger.error(f'Cannot open repository {config["repository"]}')
        logger.exception(e)
        return None, None, None, None

    icsSha = None
    jsonSha = None
    for file in icsDir:
        if file.name == config['ics_file']:
            icsSha = file.sha
        elif file.name == config['json_file']:
            jsonSha = file.sha

    if not icsSha:
        logger.warning(f'No previous ICS file {config["ics_file"]} present!')
    if not jsonSha:
        logger.warning(f'No previous JSON file {config["json_file"]} present!')
        return repo, icsSha, None, None

    jsonFileContent = None
    try:
        jsonFileContent = repo.get_git_blob(jsonSha)
        jsonFileContent = b64decode(jsonFileContent.content)
    except GithubException as e:
        logger.error(f'Cannot download previous JSON file {config["json_file"]}!')
        logger.exception(e)

    return repo, icsSha, jsonSha, jsonFileContent


def storeToGit(repo, config, icsSha, icsFileContent, jsonSha, jsonFileContent):
    try:
        if icsSha:
            repo.update_file(config['ics_file'],
                             f'Update {config["ics_file"]}',
                             icsFileContent.to_ical(),
                             icsSha)
        else:
            repo.create_file(config['ics_file'],
                             f'Initialize {config["ics_file"]}',
                             icsFileContent.to_ical())

        if jsonSha:
            repo.update_file(config['json_file'],
                             f'Update {config["json_file"]}',
                             json.dumps(jsonFileContent, indent=2),
                             jsonSha)
        else:
            repo.create_file(config['json_file'],
                             f'Initialize {config["json_file"]}',
                             json.dumps(jsonFileContent, indent=2))
    except GithubException as e:
        logger.error(f'Error: cannot push {config["ics_file"]} and {config["json_file"]} to repository:')
        logger.exception(e)
        return False
    return True


def parseArgs():
    parser = argparse.ArgumentParser(description='Crawls event across departments at Georgia Tech')
    parser.add_argument('--config', '-c', type=argparse.FileType(), required=True,
                        help='Path to the parser config file')
    parser.add_argument('--log', '-l', choices=('NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
                        default=None, help='Log level')
    parser.add_argument('--ignore-previous-crawls', '-i', default=False,
                        action='store_true', help='Ignore events from previous crawls')
    parser.add_argument('--force-write', '-f', default=False,
                        action='store_true', help='Force write to Github even if no new events were found')
    parser.add_argument('--option', '-o', action=OptionOverrideAction, default={},
                        dest='options', metavar='KEY=VAL', help='Adds an option override for the provided config')
    args = parser.parse_args()

    # config overrides
    args.config = yaml.load(args.config, Loader=yaml.SafeLoader)
    for (key, val) in args.options.items():
        # parse value in the same way as if the config file contained it
        parsedVal = yaml.load(val, Loader=yaml.SafeLoader)
        utils.updateDictPath(args.config, key, parsedVal, sep='/')
    return args


def main(token, args):
    config = args.config
    storageConfig = config['storage']
    rootLogger.setLevel(args.log or config['general']['log_level'].upper())

    crawler = Crawler(config)
    RawEvent.BASE_TZ = config['crawler']['defaults']['timezone']

    repo, icsSha, jsonSha, jsonContent = loadFromGit(token, storageConfig)
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

    res = storeToGit(repo, storageConfig, icsSha, exportedICS, jsonSha, exportedJSON)
    return 0 if res else 1


if __name__ == '__main__':
    token = os.environ['ACCESS_TOKEN']
    if not token:
        logger.error('No access token provided')
        exit(1)

    args = parseArgs()

    exit(main(token, args))
