#!/usr/bin/env python
"""
_Mask_

Unittest for the WMCore.DataStructs.Mask class

"""


# This code written as essentially a blank for future
# Mask development
# -mnorman
from __future__ import print_function

import unittest
from WMCore.DataStructs.Mask import Mask
from WMCore.DataStructs.Run import Run


class MaskTest(unittest.TestCase):
    """
    _MaskTest_

    """



    def testSetMaxAndSkipEvents(self):
        """
        test class for setMaxAndSkipEvents in Mask.py

        """

        testMask = Mask()
        maxEvents  = 100
        skipEvents = 10

        testMask.setMaxAndSkipEvents(maxEvents, skipEvents)

        self.assertEqual(testMask['FirstEvent'], skipEvents)
        self.assertEqual(testMask['LastEvent'],  maxEvents + skipEvents - 1)

        return


    def testSetMaxAndSkipLumis(self):
        """
        test class for setMaxAndSkipLumis in Mask.py

        """

        testMask  = Mask()
        maxLumis  = 10
        skipLumis = 2

        testMask.setMaxAndSkipLumis(maxLumis, skipLumis)

        self.assertEqual(testMask['FirstLumi'], skipLumis)
        self.assertEqual(testMask['LastLumi'],  maxLumis + skipLumis - 1)

        return


    def testSetMaxAndSkipRuns(self):
        """
        test class for setMaxAndSkipRuns in Mask.py

        """

        testMask  = Mask()
        maxRuns   = 1000
        skipRuns  = 200

        testMask.setMaxAndSkipRuns(maxRuns, skipRuns)

        self.assertEqual(testMask['FirstRun'], skipRuns)
        self.assertEqual(testMask['LastRun'],  maxRuns + skipRuns - 1)

        return

    def testGetMaxEvents(self):
        """
        test class for getMaxEvents in Mask.py

        """

        #The way I've decided to implement this depends on SetMaxAndSkipEvents()
        #Therefore a failure in one will result in a failure in the second
        #I'm not sure if this is the best way, but it's the one users will use
        #The problem is that it's called in reverse order by unittest so you have to
        #remember that.
        # -mnorman

        testMask = Mask()
        maxEvents  = 100
        skipEvents = 1

        tempMax = testMask.getMaxEvents()

        self.assertEqual(tempMax, None)

        testMask.setMaxAndSkipEvents(maxEvents, skipEvents)

        tempMax = testMask.getMaxEvents()

        self.assertEqual(tempMax, maxEvents + skipEvents - 1)


    def testGetMax(self):
        """
        test class for the getMax() routine added to Mask.py

        """

        testMask  = Mask()
        maxRuns   = 1000
        skipRuns  = 200

        testMask.setMaxAndSkipRuns(maxRuns, skipRuns)

        self.assertEqual(testMask.getMax('Event'), None)
        self.assertEqual(testMask.getMax('Lumi'), None)
        self.assertEqual(testMask.getMax('junk'), None)
        self.assertEqual(testMask.getMax('Run'),  1000)


    def testRunsAndLumis(self):
        """
        Test several different ways of creating the same list
        of runs and lumis
        """

        runMask         = Mask()
        rangesMask      = Mask()
        runAndLumisMask = Mask()

        runMask.addRun(Run(100,1,2,3,4,5,6,8,9,10))
        runMask.addRun(Run(200,6,7,8))
        runMask.addRun(Run(300,12))

        rangesMask.addRunWithLumiRanges(run=100, lumiList=[[1, 6], [8, 10]])
        rangesMask.addRunWithLumiRanges(run=200, lumiList=[[6, 8]])
        rangesMask.addRunWithLumiRanges(run=300, lumiList=[[12, 12]])

        runAndLumisMask.addRunAndLumis(run=100, lumis=[1, 6])
        runAndLumisMask.addRunAndLumis(run=100, lumis=[8, 10])
        runAndLumisMask.addRunAndLumis(run=200, lumis=[6, 8])
        runAndLumisMask.addRunAndLumis(run=300, lumis=[12, 12])

        self.assertEqual(runMask.getRunAndLumis(), rangesMask.getRunAndLumis())
        # Note, this may break if the TODO in Mask.addRunAndLumis() is addressed
        self.assertEqual(runMask.getRunAndLumis(), runAndLumisMask.getRunAndLumis())


    def testFilter(self):
        """
        Test filtering of a set(run) object
        """
        mask = Mask()
        mask.addRunWithLumiRanges(run=1, lumiList=[[1, 9], [12, 12], [31, 31], [38, 39], [49, 49]])
        print(mask)
        runs = set()
        runs.add(Run(1, 148, 166, 185, 195, 203, 212))
        newRuns = mask.filterRunLumisByMask(runs = runs)
        self.assertEqual(len(newRuns), 0)

        runs = set()
        runs.add(Run(1, 2, 148, 166, 185, 195, 203, 212))
        runs.add(Run(2, 148, 166, 185, 195, 203, 212))
        newRuns = mask.filterRunLumisByMask(runs = runs)
        self.assertEqual(len(newRuns), 1)
        run = newRuns.pop()
        self.assertEqual(run.run, 1)
        self.assertEqual(run.lumis, [2])

        runs = set()
        runs.add(Run(1, 2, 9, 148, 166, 185, 195, 203, 212))
        newRuns = mask.filterRunLumisByMask(runs=runs)
        self.assertEqual(len(newRuns), 1)
        run = newRuns.pop()
        self.assertEqual(run.run, 1)
        self.assertEqual(run.lumis, [2,9])

        mask = Mask()
        mask.addRunAndLumis(run=1, lumis=[5, 6])
        runs = set()
        runs.add(Run(1, range(1, 11)))
        newRuns = mask.filterRunLumisByMask(runs=runs)
        run = newRuns.pop()
        self.assertEqual(run.run, 1)
        self.assertEqual(run.lumis, [5, 6])


if __name__ == '__main__':
    unittest.main()
