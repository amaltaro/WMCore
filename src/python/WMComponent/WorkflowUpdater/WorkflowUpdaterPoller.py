#!/usr/bin/env python
"""
The WorkflowUpdater poller component.
Among the actions performed by this component, we can list:
* find active workflows in the agent
* filter those that require pileup dataset
* find out the current location for the pileup datasets
* get a list of blocks available and locked by WM
* match those blocks with the current pileup config json file. In other words,
  blocks that are no longer locked and/or available need to be removed from the
  json file.
* update this json in the workflow sandbox
"""

import os
import json
import logging
import tarfile
import tempfile
import threading

from Utils.CertTools import cert, ckey
from Utils.IteratorTools import flattenList
from Utils.FileTools import tarMode, findFiles
from Utils.Timers import timeFunction, CodeTimer
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.WMException import WMException
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory


def findJsonSandboxFiles(tfile):
    """
    Find location of sandbox JSON files
    :param tfile: sandbox tar file
    :return: list of file names
    """
    files = []
    mode = tarMode(tfile, 'r')
    with tarfile.open(tfile, mode, encoding='utf-8') as tar:
        for tarInfo in tar.getmembers():
            if tarInfo.name.endswith("pileupconf.json"):
                files.append(tarInfo.name)
    return files


def extractPileupconf(tfile, fname):
    """
    Extract content of given file name from sandbox tar file
    :param tfile: sandbox tar file
    :param fname: name of the file to extract
    :return: content of file
    """
    mode = tarMode(tfile, 'r')
    with tarfile.open(tfile, mode, encoding='utf-8') as tar:
        f = tar.extractfile(fname)
        data = f.read()
        # convert our data bytes into JSON object
        return json.loads(data)


def blockLocations(jdoc):
    """
    Return dict block names and their location from provided json sandbox file.

    json structure is
    {"<type: mc>": {"<blockName>": {"FileList" [], "NumberOfEvents":1, "PhEDExNodeNames": []},
                    "<blockName>": {"FileList" [], "NumberOfEvents":1, "PhEDExNodeNames": []}, ...}

    :param jdoc: JSON document
    :return: dict {'blockName': [rses], ...}
    """
    bdict = {}
    for rec in jdoc.values():
        for key in rec.keys():
            doc = rec[key]
            bdict[key] = doc['PhEDExNodeNames']
    return bdict


def rucioBlockLocations(rucio, blocks):
    """
    Return dictionary in form: {"path-to-json1": jdoc1, "path-to-json2": jdoc2}
    :param rucio: rucio client
    :param blocks: list of blocks
    :return: directionary
    """
    # rucio returns: [{"name": blockName, "replica": list(set(rses))}, ...]
    rucioList = rucio.getReplicaInfoForBlocks(block=blocks)
    bdict = {}
    for item in rucioList:
        bdict[item['name']] = item['replica']
    return bdict


def checkChanges(rdict, bdict):
    """
    Compare if provided rucio block locations are different from block locations presented
    in pileup configuration JSON file.
    :param rdict: dict of blocks locations from rucio
    :param bdict: dict of blocks locations from pileupconf.json sandbox file
    :return: boolean (i.e. if there are changes or not)
    """
    # compare block names
    if len(rdict) != len(bdict):
        return True

    # compare locations
    for key in rdict.keys():
        if sorted(rdict.get(key, [])) != sorted(bdict.get(key, [])):
            return True
    return False


def updateBlockInfo(jdoc, rdict):
    """
    Discard given pileup names from JSON sandbox and return new dict.
    NOTE: this function may consume lots of memory due to jdoc structure which
    we need to parse and create new dict since we can't discard keys in place
    of jdoc internal dictionary, i.e.

    jdoc structure is
    {"<type: mc>": {"<block>": {"FileList" [], "NumberOfEvents":1, "PhEDExNodeNames": []},
                    "<block>": {"FileList" [], "NumberOfEvents":1, "PhEDExNodeNames": []}, ...}
    and we need to discard block names. They are located in internal dictionary
    and in place pop-up of jdoc is not allowed since it will change size of internal dictionary.
    Therefore, to overcome this obstacles we need to copy relevant pileupName key:values
    into new dict structure and return new dict.

    :param jdoc: JSON sandbox dictionary
    :param rdict: dict of blocks locations from rucio
    :return: updates jdoc in place
    """
    for puType in jdoc:
        for blockName in list(jdoc[puType].keys()):
            if blockName in rdict:
                # then update the location for this block
                jdoc[puType][blockName]["PhEDExNodeNames"] = rdict[blockName]
            else:
                # then remove this block from the json document (as it is not in MSPileup)
                jdoc[puType].pop(blockName)


def writePileupJson(tfile, jsonFileName, jdict, dest=None):
    """
    Write pileup JSON sandbox files back to file system
    :param tfile: tar ball file name
    :param jsonFileName: a string with a relative path to the pileupconf.json file
    :param jdict: JSON sandbox dictionary in form: {"path-to-json1": jdoc1, "path-to-json2": jdoc2}
    :param dest: optional destination parameter to write final tar ball (use for unit tests)
    :return: nothing
    """
    bname = os.path.basename(tfile)
    dname = os.path.dirname(tfile)
    ofile = f"{dname}/new-{bname}"
    with tempfile.TemporaryDirectory() as tmpDir:
        # extract tar ball content into temporary directory
        with tarfile.open(tfile, tarMode(tfile, 'r'), encoding='utf-8') as tar:
            tar.extractall(path=tmpDir)
        # overwrite json sanbox file in temporary directory
        for jname in jdict:
            fname = os.path.join(tmpDir, jsonFileName)
            previousStat = os.stat(fname)
            with open(fname, 'w', encoding='utf-8') as ostream:
                json.dump(jdict, ostream)
            if previousStat == os.stat(fname):
                # something wrong as we did not update the file
                msg = f"File {fname} was not properly updated in {tfile}, file stat is identical"
                raise Exception(msg)
        # archive back sandbox
        with tarfile.open(ofile, tarMode(ofile, 'w'), encoding='utf-8') as tar:
            tar.add(tmpDir, arcname='')
        # overwrite existing tarball with new one
        if dest:
            os.rename(ofile, dest)
        else:
            os.rename(ofile, tfile)


class WorkflowUpdaterException(WMException):
    """
    Specific WorkflowUpdaterPoller exception handling.
    """


class WorkflowUpdaterPoller(BaseWorkerThread):
    """
    Poller that does the actual work for updating workflows.
    """

    def __init__(self, config):
        """
        Initialize WorkflowUpdaterPoller object
        :param config: a Configuration object with the component configuration
        """
        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listActiveWflows = self.daoFactory(classname="Workflow.GetUnfinishedWorkflows")

        # parse mandatory attributes from the configuration
        self.config = config
        self.rucioAcct = getattr(config.WorkflowUpdater, "rucioAccount")
        self.rucioUrl = getattr(config.WorkflowUpdater, "rucioUrl")
        self.rucioAuthUrl = getattr(config.WorkflowUpdater, "rucioAuthUrl")
        self.rucioCustomScope = getattr(config.WorkflowUpdater, "rucioCustomScope",
                                        "group.wmcore")
        self.msPileupUrl = getattr(config.WorkflowUpdater, "msPileupUrl")

        self.userCert = cert()
        self.userKey = ckey()
        try:
            self.rucio = Rucio(acct=self.rucioAcct,
                               hostUrl=self.rucioUrl,
                               authUrl=self.rucioAuthUrl)
        except Exception as ex:
            self.rucio = None

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Executed in every polling cycle. The actual logic of the component is:
          1. find active workflows in the agent
          2. check if those active workflows are using pileup data
        :param parameters: not really used. But keeping same signature as
            the one defined in the super class.
        :return: only what is returned by the decorator
        """
        logging.info("Running Workflow updater injector poller algorithm...")
        try:
            # retrieve list of workflows with unfinished Production
            # or Processing subscriptions
            wflowSpecs = self.listActiveWflows.execute()
            if not wflowSpecs:
                logging.info("Agent has no active workflows at the moment")
                return

            # figure out workflows that have pileup
            puWflows = self.findWflowsWithPileup(wflowSpecs)
            if not puWflows:
                logging.info("Agent has no active workflows with pileup at the moment")
                return
            # resolve unique active pileup dataset names
            uniqueActivePU = flattenList([item['pileup'] for item in puWflows])

            # otherwise, move on retrieving pileups
            msPileupList = self.getPileupDocs()

            # and resolve blocks in each container being used by workflows
            # considerations for 2024 are around 100 pileups each taking 2 seconds in Rucio
            with CodeTimer("Rucio block resolution", logger=logging):
                self.findRucioBlocks(uniqueActivePU, msPileupList)

            with CodeTimer("Updated pileup in workflow sandbox", logger=logging):
                self.adjustJSONSpec(puWflows, msPileupList)
        except Exception as ex:
            msg = f"Caught unexpected exception in WorkflowUpdater. Details:\n{str(ex)}"
            logging.exception(msg)
            raise WorkflowUpdaterException(msg) from None

    def adjustJSONSpec(self, puWflows, msPileupList, dest=None):
        """
        Main logic of the algorithm:
        - for every pileup record find out location of tarball
        - get configuration files within tarball
        - extract block information
        - replace pileupconf.json files within tarball
        :param puWflows: list of active pileup workflows with the following structure:
            {"name": string with workflow name,
             "spec": string with spec path,
             "pileup": list of strings with pileup names}
        :param msPileupList: list of all pileup records in MSPileup service. It has the following structure:
            {"pileupName": string with pileup name,
             "customName": string with custom pileup name - if any,
             "rses": list of RSE names,
             "blocks": list of block names}
        :param dest: optional destination parameter to write final tar ball (use for unit tests)
        :return: nothing, it performs checks and adjust in place pileupconf.json file(s)
        """
        # loop over active pileup workflows
        for wflow in puWflows:
            # this logic implies that we have untarred workflow tar ball
            sandboxDir = os.path.join(wflow['spec'], "../WMSandbox")
            self.logger.info("Processing workflow sandbox: %s", sandboxDir)

            # find the pileup files
            puFiles = findFiles(sandboxDir, "pileupconf.json")

            # list over each pileupconf.json and update it
            for puFile in puFiles:
                # load the JSON and figure out the dataset name
                with open(puFile, 'r', encoding='utf-8') as istream:
                    puJsonContent = json.load(istream)

                jsonPUName = ""
                for _puType, blocks in puJsonContent.items():
                    for blockName, content in blocks.items():
                        jsonPUName = blockName.split("#")[0]
                        break  # we already have the pileup name
                self.logger.info("Found pileup name %s under path: %s", jsonPUName, puFile)

                # now that we know the pileup name, iterate over the MSPileup docs
                for pileupDoc in msPileupList:
                    # check if active pileup workflow is found in MSPileup one
                    pileupName = pileupDoc['pileupName']
                    if pileupName == jsonPUName:
                        # then we need to check whether there are any changes or not
                        jsonBlockLoc = blockLocations(puJsonContent)
                        msPUBlockLoc = [{block: pileupDoc["rses"]} for block in pileupDoc["blocks"]]

                        # are the block locations different between the JSON and MSPileup?
                        if checkChanges(jsonBlockLoc, msPUBlockLoc):
                            self.logger.info("Found differences between JSON and MSPileup content.")
                            updateBlockInfo(puJsonContent, msPUBlockLoc)
                            # finally, update the tarball
                            # FIXME TODO: we should update a tarball only once for each pileup name,
                            # NOT once for each pileupconf.json file
                            self.logger.info("Going to update tarball %s with a fresh pileup content",
                                             wflow['spec'])
                            # We update a workflow tarball, for a specific file, with new json content
                            writePileupJson(wflow['spec'], puFile, puJsonContent, dest)
                        else:
                            msg = "There are no differences between JSON and MSPileup content "
                            msg += f"for pileup name {pileupName}. Not updating anything!"
                            self.logger.info(msg)
            self.logger.info("Done updating spec: %s\n", wflow['spec'])

    def getPileupDocs(self):
        """
        Fetch all pileup documents from MSPileup and preprocess the data.

        Note that the 'blocks' field is for the moment just a placeholder,
        as it will be populated in a later stage,

        :return: a list of dictionaries in the following format:
          {"pileupName": string with pileup name,
           "customName": string with custom pileup name - if any,
           "rses": list of RSE names,
           "blocks": list of block names}
        """
        mgr = RequestHandler()
        headers = {'Content-Type': 'application/json'}
        data = mgr.getdata(self.msPileupUrl, params={}, headers=headers, verb='GET',
                           ckey=self.userKey, cert=self.userCert, encode=True, decode=True)
        if data and data.get("result", []):
            if "error" in data["result"][0]:
                msg = f"Failed to retrieve MSPileup documents. Error: {data}"
                raise WorkflowUpdaterException(msg)

        logging.info("A total of %d pileup documents have been retrieved.", len(data["result"]))
        pileupMapList = []
        for puItem in data["result"]:
            logging.info("Pileup: %s, custom name: %s, expected at: %s, but currently available at: %s",
                         puItem['pileupName'], puItem['customName'],
                         puItem['expectedRSEs'], puItem['currentRSEs'])
            thisPU = {"pileupName": puItem['pileupName'],
                      "customName": puItem['customName'],
                      "rses": puItem['currentRSEs'],
                      "blocks": []}
            pileupMapList.append(thisPU)
        return pileupMapList

    def findWflowsWithPileup(self, listSpecs):
        """
        Given a list of workflow names and their respective specs, load each
        one of them and filter out those that don't require any pileup dataset.
        :param listSpecs: a list of dictionary with workflow name and spec path
        :return: a list of dictionaries with workflow name, spec path and list
            of pileup datasets being used, e.g.:
            {"name": string with workflow name,
             "spec": string with spec path,
             "pileup": list of strings with pileup names}
        """
        wflowsWithPU = []
        for wflowSpec in listSpecs:
            try:
                workloadHelper = WMWorkloadHelper()
                workloadHelper.load(wflowSpec['spec'])
                pileupSpecs = workloadHelper.listPileupDatasets()
                if pileupSpecs:
                    wflowSpec['pileup'] = pileupSpecs.values()
                    logging.info("Workflow: %s requires pileup dataset(s): %s",
                                 wflowSpec['name'], wflowSpec['pileup'])
                    wflowsWithPU.append(wflowSpec)
                else:
                    logging.info("Workflow: %s does not require any pileup", wflowSpec['name'])
            except Exception as ex:
                msg = f"Failed to load spec file for: {wflowSpec['spec']}. Details: {str(ex)}"
                logging.error(msg)
        logging.info("There are %d pileup workflows out of %d active workflows.",
                     len(wflowsWithPU), len(listSpecs))
        return wflowsWithPU

    def findRucioBlocks(self, uniquePUList, msPileupList):
        """
        Given a list of unique pileup dataset names, list all of
        their blocks in Rucio. Note that if a pileup document contains
        a customName dataset, then we need to resolve the blocks for that
        instead.
        :param uniquePUList: a list with pileup names
        :param msPileupList: a list with dictionaries from MSPileup
        :return: update the msPileupList object in place, by populating
            the 'block' field with a list of block names
        """
        for pileupItem in msPileupList:
            if pileupItem["pileupName"] not in uniquePUList:
                # no active workflow requires this pileup
                continue

            if pileupItem["customName"]:
                logging.info("Fetching blocks for custom pileup container: %s", pileupItem["customName"])
                pileupItem["blocks"] = self.rucio.getBlocksInContainer(pileupItem["customName"],
                                                                       scope=self.rucioCustomScope)
            else:
                logging.info("Fetching blocks for pileup container: %s", pileupItem["pileupName"])
                pileupItem["blocks"] = self.rucio.getBlocksInContainer(pileupItem["pileupName"], scope='cms')
