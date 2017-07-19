#!/usr/bin/env python
#-*- coding:utf-8 -*-
import logging
import pprint

from json_tools.diff import diff
from json_tools.patch import patch
from json_tools.path import split, join, resolve


def test_simple_diff():
    local = {'foo': 1, 'bar': 2}
    other = {'foo': 2, 'baz': 3}
    delta = diff(local, other)
    logging.debug('delta == %s', pprint.pformat(delta))
    assert delta == [
        {'prev': 1, 'value': 2, 'replace': '/foo'},
        {'prev': 2, 'remove': '/bar'},
        {'add': '/baz', 'value': 3}
    ]


def test_nested_diff():
    local = {'foo': {'bar': 1, 'baz': 2}}
    other = {'foo': {'bar': 2, 'qux': 3}}
    delta = diff(local, other)
    logging.debug('delta == %s', pprint.pformat(delta))
    assert delta == [
        {'prev': 2, 'remove': '/foo/baz'},
        {'prev': 1, 'replace': '/foo/bar', 'value': 2},
        {'add': '/foo/qux', 'value': 3}
    ]


def test_nested_array_diff():
    local = {'foo': [{'bar': 1, 'baz': 2}, {'qux': 3, 'quux': 4}]}
    other = {'foo': [{'bar': 2, 'qux': 3}, {'quux': 4, 'corge': 5}]}
    delta = diff(local, other)
    logging.debug('delta == %s', pprint.pformat(delta))
    assert delta == [
        {'prev': 2, 'remove': '/foo/0/baz'},
        {'prev': 1, 'replace': '/foo/0/bar', 'value': 2},
        {'add': '/foo/0/qux', 'value': 3},
        {'prev': 3, 'remove': '/foo/1/qux'},
        {'add': '/foo/1/corge', 'value': 5}
    ]


def test_simple_patch():
    local = {'foo': 1, 'bar': 2}
    other = {'foo': 2, 'baz': 3}
    delta = diff(local, other)
    patched = patch(local, delta)
    logging.debug('delta == %s', pprint.pformat(delta))
    logging.debug('patched == %s', pprint.pformat(patched))
    assert patched == other


def test_nested_patch():
    local = {'foo': {'bar': 1, 'baz': 2}}
    other = {'foo': {'bar': 2, 'qux': 3}}
    delta = diff(local, other)
    patched = patch(local, delta)
    logging.debug('patched == %s', pprint.pformat(patched))
    assert patched == other


def test_nested_array_patch():
    local = {'foo': [{'bar': 1, 'baz': 2}, {'qux': 3, 'quux': 4}]}
    other = {'foo': [{'bar': 2, 'qux': 3}, {'quux': 4, 'corge': 5}]}
    delta = diff(local, other)
    logging.debug('delta == %s', pprint.pformat(delta))
    patched = patch(local, delta)
    logging.debug('patched == %s', pprint.pformat(patched))
    assert patched == other


def test_root_array():
    doc = [1, {'a': 2}]
    p = [{'add': '/1/b', 'value': 'test'}]
    assert patch(doc, p) == [1, {'a': 2, 'b': 'test'}]


def test_create_array():
    doc = {'a': 1}
    p = [{'add': '$.b[5]', 'value': 1}]
    assert patch(doc, p) == {'a': 1, 'b': [None] * 5 + [1]}

    doc = {}
    p = [{'add': '/b/2/1/3/a', 'value': 2}]
    assert patch(doc, p) == {'b': [None, None, [None, [None, None, None, {'a': 2}]]]}


if __name__ == '__main__':
    import nose
    nose.runmodule()
