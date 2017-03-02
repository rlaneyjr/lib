from setuptools import setup

setup(
    name='spiffy',
    version='0.1',
    py_modules=['spiffy'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        spiffy=spiffy:spiffy
    ''',
)