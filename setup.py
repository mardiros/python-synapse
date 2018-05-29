#!/usr/bin/env python
import os
import re

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
# CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

with open(os.path.join(here, 'synapse', '__init__.py')) as v_file:
    version = re.compile(r".*__version__ = '(.*?)'",
                         re.S).match(v_file.read()).group(1)

requires = ['pyzmq==14.4.0',
            'redis',
            'gevent==1.1.1',
            'simplejson']

setup(name='synapse',
      version=version,
      description='Distributed communication module',
      long_description=README,  # + '\n\n' +  CHANGES,
      author='Greg Leclercq',
      author_email='greg@0x80.net',
      url='http://github.org/ggreg/python-synapse',
      packages=find_packages(),
      install_requires=requires,
      tests_require=requires,
      zip_safe=False,
      test_suite='synapse'
      )
