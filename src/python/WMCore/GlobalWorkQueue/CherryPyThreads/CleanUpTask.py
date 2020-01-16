from __future__ import (division, print_function)

from time import time
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue

class CleanUpTask(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(CleanUpTask, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.cleanUpAndSyncCanceledElements, 'duration': config.cleanUpDuration}]

    def cleanUpAndSyncCanceledElements(self, config):
        """
        1. deleted the wqe in end states
        2. synchronize cancelled elements.
        We can also make this in the separate thread
        """
        self.logger.info("Executing workqueue cleanup and sync task...")
        start = int(time())
        globalQ = globalQueue(**config.queueParams)
        globalQ.performQueueCleanupActions(skipWMBS=True)
        end = int(time())
        self.logger.info("%s executed in %d secs.", self.__class__.__name__, end - start)
        return
