import luigi as lg
import logging
from plumbum import FG

logger = logging.getLogger('luigi-interface')

class Task(lg.Task):

    # Expected to get a plumbem object
    def ex(self, chain):
        logger.info("EX: {}".format(chain))
        chain & FG

