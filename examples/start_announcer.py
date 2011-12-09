#!/usr/bin/env python
"""
Script that start an anouncer server

Usage:
  Open a shell in the parent directory of this file.

::

    $ export PYTHONPATH=.
    $ python examples/start_announcer.py
"""
import yaml

from synapse import node


if __name__ == '__main__':
    import logging
    from logging import StreamHandler
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    stream = StreamHandler()
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    common_config = yaml.load(file('examples/config.yaml'))

    announce_server_config = common_config
    with node.AnnounceServer(announce_server_config):
        node.poller.wait()
