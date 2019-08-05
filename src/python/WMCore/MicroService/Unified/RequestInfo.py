#!/usr/bin/env python
"""
_RequestInfo_

Class to hold and parse all information related to a given request
"""
# futures
from __future__ import division, print_function

# system modules
import json
import pickle
import time

# WMCore modules
from WMCore.MicroService.Unified.Common import elapsedTime, \
    cert, ckey, workflowsInfo, dbsInfo, phedexInfo, \
    eventsLumisInfo, getComputingTime, \
    getNCopies, teraBytes, getIO
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.SiteInfo import SiteInfo
from WMCore.Services.pycurl_manager import getdata \
    as multi_getdata, RequestHandler


class RequestInfo(MSCore):
    """
    RequestInfo class provides functionality to access and
    manipulate requests.
    """

    def cycleSetup(self):
        """
        Runs some preparation for the transferor cycle, such that
        we don't perform the same operation for every call to this class.
         * update the unified configuration
         * fetch all the campaign configuration with a bulk request
        :return: False if any of the updates failed, else return True
        """
        # obtain new unified Configuration
        self.uConfig = self.unifiedConfig()
        if not self.uConfig:
            self.logger.warning("Failed to fetch the latest unified config. Skipping this cycle")
            return False
        self.campaigns = self.reqmgrAux.getCampaignConfig("ALL_DOCS")
        if not self.campaigns:
            self.logger.warning("Failed to fetch campaign documents in bulk. Skipping this cycle")
            return False
        return True

    def __call__(self, rawData):
        """
        Run the unified transferor box
        :param reqRecords: input records
        :return: output records
        """
        # get complete requests information (based on Unified Transferor logic)
        reqRecords = self.requestRecord(rawData)
        self.requestsInfo(reqRecords)
        requestsToProcess = self.unified(reqRecords, rawData)

        return requestsToProcess

    def requestRecord(self, wfData):
        """
        Selects only important information for a request dictionary

        Returns: a dictionary
        """
        datasets = []
        campaigns = set()
        if "TaskChain" in wfData or "StepChain" in wfData:
            innerDicts = []
            for i in range(1, wfData.get("TaskChain", wfData.get("StepChain")) + 1):
                innerDicts.append(wfData.get("Task%d" % i, wfData.get("Step%d" % i)))
        else:
            # ReReco and DQMHarvesting
            innerDicts = [wfData]
        for item in innerDicts:
            campaign = item.get("Campaign")
            if campaign:
                campaigns.add(campaign)
            for key in self.inputType:
                dataset = item.get(key)
                if dataset:
                    datasets.append({'type': key, 'name': dataset})

        return {'name': wfData.get('RequestName'),
                'reqStatus': wfData.get('RequestStatus'),
                'SiteWhiteList': wfData.get('SiteWhitelist', []),
                'SiteBlackList': wfData.get('SiteBlacklist', []),
                'datasets': datasets,
                'campaigns': list(campaigns)}

    def requestsInfo(self, reqRecords):
        """
        Helper function to get information about all requests
        """
        # get campaigns for all requests which will be used to decide
        # how many replicas have to be made and where data has to be subscribed to
        for req in reqRecords:
            self.logger.info("request: %s, campaigns: %s", req['name'], req['campaigns'])
            for camp in self.campaigns:
                if camp['CampaignName'] in req['campaigns']:
                    self.logger.debug("    campaign configuration:: %s", camp)

    def unified(self, reqRecords, rawData):
        """
        Unified Transferor black box
        :param reqRecords: input records pre-processed
        :param wfData: list of raw request dictionary
        """
        # get aux info for dataset/blocks from inputs/parents/pileups
        # make subscriptions based on site white/black lists
        self.logger.info("unified processing %d requests", len(reqRecords))

        # TODO: the logic below shows original unified port and it should be
        #       revisited wrt new proposal specs and unified codebase

        orig = time.time()
        # get workflows info summaries and collect datasets we need to process
        winfo = workflowsInfo(rawData)
        datasets = [d for row in winfo.values() for d in row['datasets']]

        # find dataset info
        time0 = time.time()
        datasetBlocks, datasetSizes = dbsInfo(datasets, self.msConfig['dbsUrl'])
        self.logger.debug(elapsedTime(time0, "### dbsInfo"))

        # find block nodes information for our datasets
        time0 = time.time()
        blockNodes = phedexInfo(datasets, self.msConfig['phedexUrl'])
        self.logger.debug(elapsedTime(time0, "### phedexInfo"))

        # find events-lumis info for our datasets
        time0 = time.time()
        eventsLumis = eventsLumisInfo(datasets, self.msConfig['dbsUrl'])
        self.logger.debug(elapsedTime(time0, "### eventsLumisInfo"))

        # get specs for all requests and re-use them later in getSiteWhiteList as cache
        requests = [r['name'] for r in reqRecords]
        reqSpecs = self._getRequestSpecs(requests)

        # get siteInfo instance once and re-use it later, it is time-consumed object
        siteInfo = SiteInfo(self.uConfig)

        requestsToProcess = []
        tst0 = time.time()
        totBlocks = totEvents = totSize = totCpuT = 0
        for wspec in rawData:
            wname = wspec['RequestName']
            time0 = time.time()
            cput = getComputingTime(wspec, eventsLumis=eventsLumis, dbsUrl=self.msConfig['dbsUrl'], logger=self.logger)
            ncopies = getNCopies(cput)

            attrs = winfo[wname]
            ndatasets = len(attrs['datasets'])
            npileups = len(attrs['pileups'])
            nblocks = nevts = nlumis = size = 0
            nodes = set()
            for dataset in attrs['datasets']:
                blocks = datasetBlocks[dataset]
                for blk in blocks:
                    for node in blockNodes.get(blk, []):
                        nodes.add(node)
                nblocks += len(blocks)
                size += datasetSizes[dataset]
                edata = eventsLumis.get(dataset, {'num_event': 0, 'num_lumi': 0})
                nevts += edata['num_event']
                nlumis += edata['num_lumi']
            totBlocks += nblocks
            totEvents += nevts
            totSize += size
            totCpuT += cput
            sites = json.dumps(sorted(list(nodes)))
            self.logger.debug("### %s", wname)
            self.logger.debug(
                    "%s datasets, %s blocks, %s bytes (%s TB), %s nevts, %s nlumis, cput %s, copies %s, %s",
                    ndatasets, nblocks, size, teraBytes(size), nevts, nlumis, cput, ncopies, sites)
            # find out which site can serve given workflow request
            t0 = time.time()
            lheInput, primary, parent, secondary, allowedSites \
                = self._getSiteWhiteList(wspec, siteInfo, reqSpecs)
            if not isinstance(primary, list):
                primary = [primary]
            if not isinstance(secondary, list):
                secondary = [secondary]
            wflowDatasets = primary + secondary
            wflowDatasetsBlocks = []
            for dset in wflowDatasets:
                for item in datasetBlocks.get(dset, []):
                    wflowDatasetsBlocks.append(item)
            rdict = dict(name=wname, datasets=wflowDatasets,
                         blocks=wflowDatasetsBlocks,
                         npileups=npileups, size=size,
                         nevents=nevts, nlumis=nlumis, cput=cput, ncopies=ncopies,
                         sites=sites, allowedSites=allowedSites, parent=parent,
                         lheInput=lheInput, primary=primary, secondary=secondary)
            requestsToProcess.append(rdict)
            self.logger.debug(elapsedTime(t0, "### getSiteWhiteList"))

        self.logger.debug("total # of workflows %s, datasets %s, blocks %s, evts %s, size %s (%s TB), cput %s (hours)",
                          len(winfo.keys()), len(datasets), totBlocks, totEvents, totSize, teraBytes(totSize), totCpuT)
        self.logger.debug(elapsedTime(tst0, '### workflows info'))
        self.logger.debug(elapsedTime(orig, '### total time'))
        return requestsToProcess

    def _getRequestWorkflows(self, requestNames):
        "Helper function to get all specs for given set of request names"
        urls = [str('%s/data/request/%s' % (self.msConfig['reqmgrUrl'], r)) for r in requestNames]
        self.logger.debug("getRequestWorkflows")
        for u in urls:
            self.logger.debug("url %s", u)
        data = multi_getdata(urls, ckey(), cert())
        rdict = {}
        for row in data:
            req = row['url'].split('/')[-1]
            try:
                data = json.loads(row['data'])
                rdict[req] = data['result'][0]  # we get back {'result': [workflow]} dict
            except Exception as exp:
                self.logger.error("fail to process row %s", row)
                self.logger.exception("fail to load data as json record, error=%s", str(exp))
        return rdict

    def _getRequestSpecs(self, requestNames):
        "Helper function to get all specs for given set of request names"
        urls = [str('%s/%s/spec' % (self.msConfig['reqmgrCacheUrl'], r)) for r in requestNames]
        data = multi_getdata(urls, ckey(), cert())
        rdict = {}
        for row in data:
            req = row['url'].split('/')[-2]
            rdict[req] = pickle.loads(row['data'])
        return rdict

    def _getSiteWhiteList(self, request, siteInfo, reqSpecs=None, pickone=False):
        "Return site list for given request"
        lheinput, primary, parent, secondary = getIO(request, self.msConfig['dbsUrl'])
        allowedSites = []
        if lheinput:
            allowedSites = sorted(siteInfo.sites_eos)
        elif secondary:
            if self.heavyRead(request):
                allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_with_goodIO))
            else:
                allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_with_goodAAA))
        elif primary:
            allowedSites = sorted(set(siteInfo.sites_T1s + siteInfo.sites_T2s + siteInfo.sites_T3s))
        else:
            # no input at all all site should contribute
            allowedSites = sorted(set(siteInfo.sites_T2s + siteInfo.sites_T1s + siteInfo.sites_T3s))
        if pickone:
            allowedSites = sorted([siteInfo.pick_CE(allowedSites)])

        # do further restrictions based on memory
        # do further restrictions based on blow-up factor
        minChildJobPerEvent, rootJobPerEvent, blowUp = self._getBlowupFactors(request, reqSpecs=reqSpecs)
        maxBlowUp, neededCores = self.uConfig.get('blow_up_limits', (0, 0))
        if blowUp > maxBlowUp:
            # then restrict to only sites with >4k slots
            siteCores = [site for site in allowedSites
                         if siteInfo.cpu_pledges[site] > neededCores]
            newAllowedSites = list(set(allowedSites) & set(siteCores))
            if newAllowedSites:
                allowedSites = newAllowedSites
                msg = "restricting site white list because of blow-up factor: "
                msg += 'minChildJobPerEvent=%s ' % minChildJobPerEvent
                msg += 'rootJobPerEvent=%s' % rootJobPerEvent
                msg += 'maxBlowUp=%s' % maxBlowUp
                self.logger.debug(msg)

        for campaign in self.getCampaigns(request):
            # for testing purposes add post campaign call
            # res = reqmgrAux.postCampaignConfig(campaign, {'%s_name' % campaign: {"Key1": "Value1"}})
            campaignConfig = self.reqmgrAux.getCampaignConfig(campaign)
            if isinstance(campaignConfig, list):
                campaignConfig = campaignConfig[0]
            campSites = campaignConfig.get('SiteWhitelist', [])
            if campSites:
                msg = "Using site whitelist restriction by campaign=%s " % campaign
                msg += "configuration=%s" % sorted(campSites)
                self.logger.debug(msg)
                allowedSites = list(set(allowedSites) & set(campSites))
                if not allowedSites:
                    allowedSites = list(campSites)

            campBlackList = campaignConfig.get('SiteBlacklist', [])
            if campBlackList:
                self.logger.debug("Reducing the whitelist due to black list in campaign configuration")
                self.logger.debug("Removing %s", campBlackList)
                allowedSites = list(set(allowedSites) - set(campBlackList))

        ncores = self.getMulticore(request)
        memAllowed = siteInfo.sitesByMemory(float(request['Memory']), maxCore=ncores)
        if memAllowed is not None:
            msg = "sites allowing %s " % request['Memory']
            msg += "MB and ncores=%s" % ncores
            msg += "core are %s" % sorted(memAllowed)
            self.logger.debug(msg)
            # mask to sites ready for mcore
            if ncores > 1:
                memAllowed = list(set(memAllowed) & set(siteInfo.sites_mcore_ready))
            allowedSites = list(set(allowedSites) & set(memAllowed))
        return lheinput, list(primary), list(parent), list(secondary), list(sorted(allowedSites))

    def _getBlowupFactors(self, request, reqSpecs=None):
        "Return blowup factors for given request"
        if request['RequestType'] != 'TaskChain':
            return 1., 1., 1.
        minChildJobPerEvent = None
        rootJobPerEvent = None
        maxBlowUp = 0
        splits = self._getSplittings(request, reqSpecs=reqSpecs)
        for item in splits:
            cSize = None
            pSize = None
            task = item['splittingTask']
            for key in ['events_per_job', 'avg_events_per_job']:
                if key in item:
                    cSize = item[key]
            parents = [s for s in splits
                       if task.startswith(s['splittingTask']) and task != s['splittingTask']]
            if parents:
                for parent in parents:
                    for key in ['events_per_job', 'avg_events_per_job']:
                        if key in parent:
                            pSize = parent[key]
                    if not minChildJobPerEvent or minChildJobPerEvent > cSize:
                        minChildJobPerEvent = cSize
            else:
                rootJobPerEvent = cSize
            if cSize and pSize:
                blowUp = float(pSize) / cSize
                if blowUp > maxBlowUp:
                    maxBlowUp = blowUp
        return minChildJobPerEvent, rootJobPerEvent, maxBlowUp

    def _getSplittings(self, request, reqSpecs=None):
        "Return splittings for given request"
        spl = []
        for task in self.getWorkTasks(request, reqSpecs=reqSpecs):
            tsplit = task.input.splitting
            spl.append({"splittingAlgo": tsplit.algorithm, "splittingTask": task.pathName})
            get_those = ['events_per_lumi', 'events_per_job', 'lumis_per_job',
                         'halt_job_on_file_boundaries', 'job_time_limit',
                         'halt_job_on_file_boundaries_event_aware']
            translate = {'EventAwareLumiBased': [('events_per_job', 'avg_events_per_job')]}
            include = {'EventAwareLumiBased': {'halt_job_on_file_boundaries_event_aware': 'True'},
                       'LumiBased': {'halt_job_on_file_boundaries': 'True'}}
            if tsplit.algorithm in include:
                for key, val in include[tsplit.algorithm].items():
                    spl[-1][key] = val
            for get in get_those:
                if hasattr(tsplit, get):
                    setTo = get
                    if tsplit.algorithm in translate:
                        for src, des in translate[tsplit.algorithm]:
                            if src == get:
                                setTo = des
                                break
                    spl[-1][setTo] = getattr(tsplit, get)
        return spl

    def getWorkTasks(self, request, reqSpecs=None):
        "Return work tasks for given request"
        select = {'taskType': ['Production', 'Processing', 'Skim']}
        allTasks = []
        tasks = self.getSpec(request, reqSpecs).tasks
        for task in tasks.tasklist:
            node = getattr(tasks, task)
            allTasks.extend(self.taskDescending(node, select))
        return allTasks

    def getSpec(self, request, reqSpecs=None):
        "Get request from workload cache"
        if reqSpecs and request['RequestName'] in reqSpecs:
            return reqSpecs[request['RequestName']]
        url = str('%s/%s/spec' % (self.msConfig['reqmgrCacheUrl'], request['RequestName']))
        mgr = RequestHandler()
        data = mgr.getdata(url, params={}, cert=cert(), ckey=ckey())
        return pickle.loads(data)

    def taskDescending(self, node, select=None):
        "Helper function to walk through task nodes in descending order"
        allTasks = []
        if not select:
            allTasks.append(node)
        else:
            for key, value in select.items():
                if (isinstance(value, list) and getattr(node, key) in value) or \
                        (not isinstance(value, list) and getattr(node, key) == value):
                    allTasks.append(node)
                    break

        for child in node.tree.childNames:
            chItem = getattr(node.tree.children, child)
            allTasks.extend(self.taskDescending(chItem, select))
        return allTasks

    def getCampaigns(self, request):
        "Return campaigns of given request"
        if 'Chain' in request['RequestType'] and not self.isRelval(request):
            return list(set(self.collectinchain(request, 'AcquisitionEra').values()))
        return [request['Campaign']]

    def heavyRead(self, request):
        """
        Return True by default. False if 'premix' appears in the
        output datasets or in the campaigns
        """
        response = True
        if any(['premix' in c.lower() for c in self.getCampaigns(request)]):
            response = False
        if any(['premix' in o.lower() for o in request['OutputDatasets']]):
            response = False
        return response

    def isRelval(self, request):
        "Return if given request is RelVal sample"
        if 'SubRequestType' in request and 'RelVal' in request['SubRequestType']:
            return True
        return False

    def collectinchain(self, request, member, func=None, default=None):
        "Helper function to return dictionary of collection chain"
        if request['RequestType'] == 'StepChain':
            return self.collectionHelper(request, member, func, default, base='Step')
        elif request['RequestType'] == 'TaskChain':
            return self.collectionHelper(request, member, func, default, base='Task')
        else:
            raise Exception("should not call collectinchain on non-chain request")

    def collectionHelper(self, request, member, func=None, default=None, base=None):
        "Helper function to return uhm chain as a dictionary"
        coll = {}
        item = 1
        while '%s%d' % (base, item) in request:
            if member in request['%s%d' % (base, item)]:
                if func:
                    coll[request['%s%d' % (base, item)]['%sName' % base]] = \
                        func(request['%s%d' % (base, item)][member])
                else:
                    coll[request['%s%d' % (base, item)]['%sName' % base]] = \
                        request['%s%d' % (base, item)].get(member, default)
            item += 1
        return coll

    def getMulticore(self, request):
        "Return max number of cores for a given request"
        mcores = [int(request.get('Multicore', 1))]
        if 'Chain' in request['RequestType']:
            mcoresCol = self.collectinchain(request, 'Multicore', default=1)
            mcores.extend([int(v) for v in mcoresCol.values()])
        return max(mcores)
