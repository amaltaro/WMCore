'''

'''
from __future__ import (division, print_function)
import time
from memory_profiler import profile

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WMStats.DataStructs.DataCache import DataCache
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

class DataCacheUpdate(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(DataCacheUpdate, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.gatherActiveDataStats, 'duration': 300}]

    @profile
    def gatherActiveDataStats(self, config):
        """
        gather active data statistics
        """
        tStart = time.time()
        try:
            if DataCache.islatestJobDataExpired():
                wmstatsDB = WMStatsReader(config.wmstats_url, reqdbURL=config.reqmgrdb_url,
                                          reqdbCouchApp="ReqMgr")
                jobData = wmstatsDB.getActiveData(jobInfoFlag = False)
                DataCache.setlatestJobData(jobData)
                self.logger.info("DataCache is updated: %s", len(jobData))
                DataCache.summary()
            else:
                self.logger.info("DataCache hasn't expired yet.")
        except Exception as ex:
            self.logger.exception(str(ex))
        self.logger.info("DataCache id: %s", id(DataCache))
        self.logger.info("DataCache.cacheContent id: %s", id(DataCache.cacheContent))
        print("Total time executing this cycle: {}".format(time.time() - tStart))
        return
