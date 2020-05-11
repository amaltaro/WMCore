import time
import gc
from memory_profiler import profile
from pprint import pformat

from WMCore.ReqMgr.DataStructs.Request import RequestInfo, protectedLFNs

class DataCache(object):
    # TODO: need to change to  store in  db instead of storing in the memory
    # When mulitple server run for load balancing it could have different result
    # from each server.
    cacheDuration = 300  # 5 minutes
    cacheUpdated = 0
    cacheContent = []

    @staticmethod
    def getDuration():
        return DataCache.cacheDuration

    @staticmethod
    def setDuration(sec):
        DataCache.cacheDuration = sec

    @staticmethod
    @profile
    def getlatestJobData():
        if (DataCache.cacheContent):
            return DataCache.cacheContent
        else:
            return {}

    @staticmethod
    def isEmpty():
        # simple check to see if the data cache is populated
        return not DataCache.cacheContent

    @staticmethod
    def summary():
        print("DataCache type: {}".format(type(DataCache)))
        print("DataCache id: {}".format(id(DataCache)))
        print("DataCache.cacheContent id: {}".format(id(DataCache.cacheContent)))

        print("Is DataCache tracked by gc: {}".format(gc.is_tracked(DataCache)))
        print("Is DataCache.cacheContent tracked by gc: {}".format(gc.is_tracked(DataCache.cacheContent)))

    @staticmethod
    @profile
    def reset():
        print("Clearing {} elements from data cache".format(len(DataCache.cacheContent)))
        del DataCache.cacheContent

    @staticmethod
    @profile
    def garbageCollect():
        # why 2 times???
        for i in range(2):
            print('Running garbage collection: {} ...'.format(i))
            res = gc.collect()
            print('Unreachable objects: {}'.format(res))
            res = gc.garbage
            print('Remaining Garbage: {}'.format(pformat(res)))

    @staticmethod
    @profile
    def setlatestJobData(jobData):
        DataCache.reset()
        DataCache.cacheContent = jobData
        DataCache.garbageCollect()

    @staticmethod
    @profile
    def islatestJobDataExpired():
        if not DataCache.cacheContent:
            return True

        if (int(time.time()) - DataCache.cacheUpdated) > DataCache.cacheDuration:
            return True
        return False

    @staticmethod
    @profile
    def filterData(filterDict, maskList):
        reqData = DataCache.getlatestJobData()

        for item in reqData:
            for _, reqInfo in item.iteritems():
                reqData = RequestInfo(reqInfo)
                if reqData.andFilterCheck(filterDict):
                    for prop in maskList:
                        result = reqData.get(prop, [])

                        if isinstance(result, list):
                            for value in result:
                                yield value
                        elif result is not None and result != "":
                            yield result

    @staticmethod
    @profile
    def filterDataByRequest(filterDict, maskList=None):
        reqData = DataCache.getlatestJobData()

        if maskList is not None:
            if isinstance(maskList, basestring):
                maskList = [maskList]
            if "RequestName" not in maskList:
                maskList.append("RequestName")

        for _, reqDict in reqData.iteritems():
            reqInfo = RequestInfo(reqDict)
            if reqInfo.andFilterCheck(filterDict):

                if maskList is None:
                    yield reqDict
                else:
                    resultItem = {}
                    for prop in maskList:
                        resultItem[prop] = reqInfo.get(prop, None)
                    yield resultItem

    @staticmethod
    @profile
    def getProtectedLFNs():
        reqData = DataCache.getlatestJobData()

        for _, reqInfo in reqData.iteritems():
            for dirPath in protectedLFNs(reqInfo):
                yield dirPath