from __future__ import absolute_import
import os, sys
import logging
import datetime

from . import uuid


def config_logging(level=logging.INFO, prefix='.', to_file=False):
    kwargs = {}
    if to_file:
        now = datetime.datetime.now().strftime('%Y%m%d%H%M%S-%f')
        rand = uuid.generate_uuid()
        filename = os.path.basename(sys.argv[0]) + '--' + now + '--' + rand + '.log'
        filepath = os.path.abspath(os.path.join(prefix, filename))
        kwargs['filename'] = filepath
    formatter = '%(asctime)s %(pathname)s[line:%(lineno)d] %(levelname)s %(message)s'
    logging.basicConfig(level=level, format=formatter, **kwargs)
    if to_file:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(formatter))
        logging.getLogger().addHandler(h)
        logging.critical('log is being written to "%s"' % filepath)
    logging.critical('command line argument: ' + str(sys.argv))
