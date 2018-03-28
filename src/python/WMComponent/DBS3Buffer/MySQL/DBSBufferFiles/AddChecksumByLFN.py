#!/usr/bin/env python

"""
MySQL implementation of AddChecksumByLFN
"""




from Utils.IteratorTools import grouper
from WMCore.Database.DBFormatter import DBFormatter

class AddChecksumByLFN(DBFormatter):
    sql = """INSERT IGNORE INTO dbsbuffer_file_checksums (fileid, typeid, cksum)
             SELECT (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn),
             (SELECT id FROM dbsbuffer_checksum_type WHERE type = :cktype), :cksum FROM dual"""

    def execute(self, lfn = None, cktype = None, cksum = None, bulkList = None, conn = None,
                transaction = False):

        if bulkList:
            binds = bulkList
        else:
            binds = [{'lfn': lfn, 'cktype': cktype, 'cksum': cksum}]

        for sliceBinds in grouper(binds, 10000):
            self.dbi.processData(self.sql, sliceBinds,
                                 conn = conn, transaction = transaction)

        return
