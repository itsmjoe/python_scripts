# -*- coding: utf-8 -*-
import logging
import sys

###########################################################
#######  COFIGURATION
###########################################################
for _ in ("boto", "elasticsearch", "urllib3"):
    logging.getLogger(_).setLevel(logging.CRITICAL)
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [stdout_handler]
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=handlers
)
