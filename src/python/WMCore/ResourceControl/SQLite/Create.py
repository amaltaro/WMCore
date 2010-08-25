#!/usr/bin/python

"""
_Create_

Class for creating SQLite specific schema for resource control.

"""

__revision__ = "$Id: Create.py,v 1.1 2009/10/05 20:03:01 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating SQLite specific schema for resource control.
    """
    
    
    
    def __init__(self):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}

        self.create['rc1_site'] = """
        CREATE TABLE rc_site(
            site_index  INTEGER       PRIMARY KEY AUTOINCREMENT,
            site_name   VARCHAR(255)  NOT NULL,
            se_name     VARCHAR(255)  NOT NULL,
            ce_name     VARCHAR(255),
            is_active   VARCHAR(5)    DEFAULT 'true',
            UNIQUE(site_name)
            )"""

        self.create['rc2_site_threshold'] = """
        CREATE TABLE rc_site_threshold(
            site_index      INTEGER      NOT NULL,
            threshold_name  VARCHAR(255) NOT NULL,
            threshold_value INT(11)      DEFAULT 0,
            UNIQUE (threshold_name, site_index),
            FOREIGN KEY (site_index) REFERENCES rc_site(site_index) ON DELETE CASCADE
            )"""

        self.create['rc3_site_attr'] = """
        CREATE TABLE rc_site_attr(
            site_index INTEGER      NOT NULL,
            attr_name  VARCHAR(255) NOT NULL,
            attr_value VARCHAR(255) DEFAULT '',
            UNIQUE (attr_name, site_index),
            FOREIGN KEY (site_index) REFERENCES rc_site(site_index) ON DELETE CASCADE
            )"""
