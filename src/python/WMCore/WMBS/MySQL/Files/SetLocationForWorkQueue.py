#!/usr/bin/env python
"""
_SetLocationForWorkQueue_

MySQL implementation of Files.SetLocationForWorkQueue

For WorkQueue only
"""

from Utils.IteratorTools import grouper
from WMCore.Database.DBFormatter import DBFormatter


class SetLocationForWorkQueue(DBFormatter):
    """
    _SetLocationForWorkQueue_

    Set the location for a file, deleting all previous references
    and attaching the current references
    """

    deleteSQL = """DELETE FROM wmbs_file_location
                     WHERE fileid = (SELECT wfd.id FROM wmbs_file_details wfd WHERE wfd.lfn = :lfn)"""

    insertSQL = """INSERT IGNORE INTO wmbs_file_location (fileid, pnn)
                     SELECT wfd.id, wpnn.id
                     FROM wmbs_pnns wpnn, wmbs_file_details wfd
                     WHERE wpnn.pnn = :location
                     AND wfd.lfn = :lfn"""

    def execute(self, lfns, locations, isDBS=True, conn=None, transaction=None):
        """
        If files are from DBS:
        First, delete all file_location references with that lfn.
        Then, insert the new ones.

        Else:
        Simply insert the new locations
        """
        if isDBS:
            binds = []
            for lfn in lfns:
                binds.add({'lfn': lfn})

            for sliceBinds in grouper(binds, 10000):
                self.dbi.processData(self.deleteSQL, sliceBinds, conn=conn,
                                     transaction=transaction)

        for sliceBinds in grouper(locations, 10000):
            self.dbi.processData(self.insertSQL, sliceBinds, conn=conn,
                                 transaction=transaction)
        return
