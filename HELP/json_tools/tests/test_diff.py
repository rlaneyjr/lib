#!/usr/bin/env python
#-*- coding:utf-8 -*-

""" Tests for 'diff' module.
"""

from json_tools.diff import diff


def test_diff_with_wrong_types():
    res = diff(3, "4")
    assert res == [{'prev': 3, 'details': 'type', 'value': '4', 'replace': '/'}]

    res = diff([], 3)
    assert res == [{'prev': [], 'details': 'type', 'value': 3, 'replace': '/'}]    

    res = diff(3, [])
    assert res == [{'prev': 3, 'details': 'type', 'value': [], 'replace': '/'}]  

    res = diff({'a': 1, 'b': 2}, {'a': '1', 'b': '2'})
    assert res == [{'prev': 1, 'details': 'type', 'value': '1', 'replace': '/a'},
                   {'prev': 2, 'details': 'type', 'value': '2', 'replace': '/b'}]