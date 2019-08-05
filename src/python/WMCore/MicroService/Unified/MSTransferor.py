"""
File       : MSTransferor.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
             Alan Malta <alan dot malta AT cern dot ch >
Description: MSTransferor class provide whole logic behind
the transferor module.
"""
# futures
from __future__ import division, print_function

# system modules
import time
from pprint import pformat

# WMCore modules
from Utils.IteratorTools import grouper
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.RequestInfo import RequestInfo
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList \
    import PhEDExSubscription


class MSTransferor(MSCore):
    """
    MSTransferor class provide whole logic behind
    the transferor module.
    """
    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MS Transferor module
        :param microConfig: microservice configuration
        """
        super(MSTransferor, self).__init__(msConfig, logger)
        self.reqInfo = RequestInfo(msConfig, logger)
        self.inputType = {'InputDataset', 'MCPileup', 'DataPileup'}

    def execute(self, reqStatus):
        """
        Executes the whole transferor logic
        :param reqStatus: request status to process
        :return:
        """
        try:
            # get requests from ReqMgr2 data-service for given statue
            requestSpecs = self.reqmgr2.getRequestByStatus([reqStatus], detail=True)
            if requestSpecs:
                requestSpecs = requestSpecs[0].values()
        except Exception as err:  # general error
            self.logger.exception('### transferor error: %s', str(err))

        self.logger.info('Found %d requests in "%s" status', len(requestSpecs), reqStatus)
        # couple of checks before we proceed with the heavy operation
        if not requestSpecs:
            return
        elif not self.reqInfo.cycleSetup():
            return

        # process all requests
        for reqSlice in grouper(requestSpecs, 50):
            requestStatuses = {}
            # get complete requests information
            # based on Unified Transferor logic
            reqResults = self.reqInfo(reqSlice)
            self.logger.info("%d requests completely processed.", len(reqResults))
            self.logger.info("Working on the data subscription and status change...")
            # process all requests
            for req in reqResults:
                reqName = req['name']
                try:
                    # perform transfer
                    sdict = self.transferRequest(req)
                    if sdict:
                        # Once all transfer requests were successfully made,
                        # update: assigned -> staging
                        self.logger.debug("### transfer request for %s successfull", reqName)
                        self.change(req, 'staging', '### transferor')
                        # if there is nothing to be transferred (no input at all),
                        # then update the request status once again staging -> staged
                        # self.change(req, 'staged', '### transferor')
                except Exception as err:  # general error
                    self.logger.exception('### transferor error: %s', str(err))
                    # compose status record
                    wname = reqName  # in our case workflow name is identical to request name
                    ctype = req['dataType']
                    for dataset, tids in sdict.items():
                        statusRecord = {'timestamp': time.time(), 'dataset': dataset,
                                        'dataType': ctype, 'transferIDs': tids}
                        requestStatuses.setdefault(wname, []).append(statusRecord)
            # update/insert requestStatues in couchdb
            self.updateTransferInfo(requestStatuses)

    def transferRequest(self, req):
        """
        Send request to Phedex and return status of request subscription
        :param req: request object
        :return: subscriptoin dictionary {"dataset":transferIDs}
        """
        datasets = req.get('datasets', [])
        sites = req.get('sites', [])
        sdict = {}
        if datasets and sites:
            self.logger.debug(
                "### creating subscription for: %s", pformat(req))
            subscription = PhEDExSubscription(
                datasets, sites, self.msConfig['group'])
            self.logger.info(
                "### TODO: perform subscription call %s", subscription)
            # TODO: when ready enable submit subscription step
            # self.phedex.subscribe(subscription)
            for dataset in datasets:
                sdict[dataset] = self.getTransferIds(dataset)
            return sdict
        return sdict
