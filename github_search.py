#!/usr/bin/env python
# Python Network Programming Cookbook -- Chapter - 6
# This program is optimized for Python 2.7.
# It may run on any other version with/without modifications.

import argparse
import requests
import json

SEARCH_URL_BASE = 'https://api.github.com/repos'


def search_repository(author, repo, search_for='homepage'):
    url = "{}/{}/{}".format(SEARCH_URL_BASE, author, repo)
    print("Searching Repo URL: {}".format(url))
    result = requests.get(url)
    if(result.ok):
        repo_info = json.loads(result.text or result.content)
        print("Github repository info for: {}".format(repo))
        result = "No result found!"
        for key, value in repo_info.iteritems():
            if search_for in key:
                result = value
            return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Github search')
    parser.add_argument('--author', action="store", dest="author",
                        required=True)
    parser.add_argument('--repo', action="store", dest="repo", required=True)
    parser.add_argument('--search_for', action="store", dest="search_for",
                        required=True)

    given_args = parser.parse_args()
    result = search_repository(given_args.author, given_args.repo,
                               given_args.search_for)
    if isinstance(result, dict):
        print("Got result for '{}'...".format(given_args.search_for))
        for key, value in result.iteritems():
            print("{} => {}".format(key, value))
    else:
        print("Got result for {}: {}".format(given_args.search_for, result))
