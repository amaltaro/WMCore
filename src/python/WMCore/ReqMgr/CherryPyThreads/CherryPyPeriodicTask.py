'''
Created on Jul 31, 2014

@author: sryu
'''
import cherrypy
import logging
from threading import Thread, Condition

class CherryPyPeriodicTask(object):
    
    def __init__(self, config):
        
        """
        BaseClass which can set up the concurrent task using cherrypy thread.
        WARNING: This assumes each task doesn't share the object. 
        (It can be share only read operation is performed)
        If the object shared by multple task and read/write operation is performed. 
        Lock is not provided for these object
    
        :arg config  WMCore.Configuration object. which need to contain in duration attr.
        TODO: add validation for config.duration
        """
        self.setConcurrentTasks()
        for task in self.concurrentTasks:
            PeriodicWorker(task, config)
        
    def setConcurrentTasks(self):
        """
        sets the list of function reference for concurrent tasks, 
        sub class should implement this
        
        each function in the list should have the same signature with
        2 arguments (self, config)
        config is WMCore.Configuration object
        """
        self.concurrentTasks = []
        raise NotImplementedError("need to implement setSequencialTas assign self._callSequence")

class PeriodicWorker(Thread):
    
    def __init__(self, func, config):
        # use default RLock from condition
        # Lock wan't be shared between the instance used  only for wait
        # func : function or callable object pointer
        self.wakeUp = Condition()
        self.stopFlag = False
        self.taskFunc = func
        self.config = config
        self.duration = config.duration
        try: 
            name = func.__class__.__name__
            print name
        except:
            name = func.__name__
            print name
        Thread.__init__(self, name=name)
        cherrypy.engine.subscribe('start', self.start, priority = 100)
        cherrypy.engine.subscribe('stop', self.stop, priority = 100)
    
        
    def stop(self):
        self.wakeUp.acquire()
        self.stopFlag = True
        self.wakeUp.notifyAll()
        self.wakeUp.release()
    
    def run(self):
        
        while not self.stopFlag:
            self.wakeUp.acquire()
            self.taskFunc(self.config)
            self.wakeUp.wait(self.duration)
            self.wakeUp.release()

class SequentialTaskBase(object):
    
    """
    Base class for the tasks which should run sequentially
    """
    def __init__(self):
        self.setCallSequence()
        
    def __call__(self, config):
        for call in self._callSequence:
            try:
                call(config)
            except Exception, ex:
                #log the excpeiotn and break. 
                #SequencialTasks are interconnected between functions  
                print (str(ex))
                logging.error(str(ex))
                break
            
    def setCallSequence(self):
        """
        set the list of function call with out args on self.callSequence
        
        i.e.
        self.callSequence = [self.do_something1, self.do_something1]
        """
        raise NotImplementedError("need to implement setCallSequence assign self._callSequence")

   
#this is the sckeleton of request data collector
class DataUploadTask(SequentialTaskBase):
    
    def setCallSequence(self):
        self._callSequence = [self.getData, self.convertData, self.putData]
    
    def getData(self, config):
        # self.data = getData(self.sourceUrl)
        pass
    
    def convertData(self, config):
        # self.data = convertData(self.data)
        pass
    
    def putData(self, config):
        # putData(self.destUrl)
        pass
