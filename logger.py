import logging
from datetime import date


# logging.basicConfig(level=logging.INFO,
#                     format="%(asctime)s - [%(levelname)s] - %(name)s - "
#                     "(%(filename)s).%(funcName)s(%(lineno)d) - %(message)s")
logging.basicConfig(filename=f'logfile_{date.today()}.log',
                    format="%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)
