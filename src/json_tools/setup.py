#!/usr/bin/env python
# coding: utf-8

from setuptools import setup

try:
    import pypandoc
    description = pypandoc.convert('README.md', 'rst')
except:
    description = ''


setup(
    name='json_tools',
    version='0.3.5',

    packages=['json_tools'],
    package_dir={'json_tools': 'json_tools'},
    install_requires=['colorama', 'six'],

    entry_points={
        'console_scripts': [
            'json = json_tools.__main__:main',
        ]
    },

    author='Vadim Semenov',
    author_email='protoss.player@gmail.com',
    url='https://bitbucket.org/vadim_semenov/json_tools',

    description='A set of tools to manipulate JSON: diff, patch, pretty-printing',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
        'License :: OSI Approved :: MIT License'        
    ],

    keywords=['json', 'json_tools', 'json-tools',
              'diff', 'json_diff', 'json-diff',
              'patch', 'json_patch', 'json-patch'],

    long_description=description
)
