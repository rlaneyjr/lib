#!/usr/bin/env python
#-*- coding:utf-8 -*-

""" Tests for 'path' module.
"""

import logging
import pprint

from json_tools.path import split, join, resolve, find, create


def test_jpath_split():
    jpath = '$.a[0].b[1].65536[2][3]'
    assert jpath == join(split(jpath))

    jpath = '$[1].a[2][3]'
    res = split(jpath)
    assert jpath == join(res)
    assert res == [
        ('array-index', 1),
        ('object-field', 'a'),
        ('array-index', 2),
        ('array-index', 3)
    ]


def test_jpointer_split():
    ptr = '/a/0/b/1'
    res = split(ptr)
    assert res == [
        ('object-field', 'a'),
        ('array-index', 0),
        ('object-field', 'b'),
        ('array-index', 1),
    ]
    assert join(res) == '$.a[0].b[1]'

    ptr = '/0/a/1/2/b'
    res = split(ptr)
    assert res == [
        ('array-index', 0),
        ('object-field', 'a'),
        ('array-index', 1),
        ('array-index', 2),
        ('object-field', 'b'),
    ]
    assert join(res) == '$[0].a[1][2].b'


def test_resolve():
    jpath = '$.a[0].b[1].65536[2][3]'
    doc = {
        'a': [
            {
                'b': [
                    'crap',
                    {
                        '65536': [
                            [],
                            ['before'],
                            [
                                None, None, 'before', 'hit', 'after'
                            ],
                            ['after']
                        ]
                    }
                ]
            },
            {
                'x': 'y'
            }
        ]
    }
    assert resolve(doc, jpath) == 'hit'
    assert resolve(doc, "/") == doc
    assert resolve(doc, "/a/") == doc['a']


def test_create():
    assert create('$.a.b.c') == {'a': {'b': {'c': None}}}
    assert create('$[1].a[2]', 'this') == [None, {'a': [None, None, 'this']}]
    assert create('$') is None
    assert create('$', 42) == 42
    assert create('/0/0/1/1/2/2/a', 42) == [[[None, [None, [None, None, [None, None, {'a': 42}]]]]]]
    assert create('/a.b') == {'a.b': None}
    assert create('$.65536[0]', 42) == {'65536': [42]}


def test_find():
    doc = {
        'a': [
            None,
            {
                'b': 3
            }
        ]
    }

    assert find(doc, '$', True) == ('$', '$', doc, None)
    assert find(doc, '$.b', True) == ('$', '$.b', doc, 'key')
    assert find(doc, '$.a.b', True) == ('$.a', '$.b', doc['a'], 'type')
    assert find(doc, '$.a[1]', True) == ('$.a[1]', '$', doc['a'][1], None)
    assert find(doc, '$.a[1].b', True) == ('$.a[1].b', '$', doc['a'][1]['b'], None)
    assert find(doc, '$.a[1].b.c', True) == ('$.a[1].b', '$.c', doc['a'][1]['b'], 'type')

    doc = [
        [
            {
                'a': {
                    'b': 42
                }
            }
        ]
    ]

    assert find(doc, '$[0].a', True) == ('$[0]', '$.a', doc[0], 'type')
    assert find(doc, '$[0][0].a', True) == ('$[0][0].a', '$', doc[0][0]['a'], None)
    assert find(doc, '$[0][0].a.c', True) == ('$[0][0].a', '$.c', doc[0][0]['a'], 'key')
    assert find(doc, '$[0][0].a.b', True) == ('$[0][0].a.b', '$', doc[0][0]['a']['b'], None)


if __name__ == '__main__':
    import nose
    nose.runmodule()
