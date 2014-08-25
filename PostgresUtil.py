
from datetime import date
import sys

from Config import *

class PostgresUtil(object):

    sqlFile = None
            
    def __init__(self, conf):
        self.conf = conf
                
    def open(self):
        try:
            sqlFilename = self.conf.get(Config.SQL_FILE)
            PostgresUtil.sqlFile = open(sqlFilename, 'w')
            PostgresUtil.sqlFile.write("BEGIN;\n")
        except:
            PostgresUtil.sqlFile = None
                
    def close(self):
        if not PostgresUtil.sqlFile is None:
            PostgresUtil.sqlFile.write("END;\n")
            PostgresUtil.sqlFile.flush()
            PostgresUtil.sqlFile.close() 
    
    def writeInsert(self, row):
        if not PostgresUtil.sqlFile is None:
            PostgresUtil.sqlFile.write("INSERT INTO %s %s VALUES %s;\n" % (row.getTableName(), row.getSchema(), row.getRow()))

    def writeCreates(self):
        if not PostgresUtil.sqlFile is None:
            rowTypes = [FsckRow(), HdfsReportRow(), HawqDBQuotaRow(), HawqDBSizeRow(), HiveDBQuotaRow(), HiveDBSizeRow(), UserSpaceQuotaRow(), \
                UserINodeQuotaRow(), UserSpaceSizeRow(), UserINodeSizeRow(), DriveUsageRow(), DriveWriteTestRow()]
            for row in rowTypes:
                PostgresUtil.sqlFile.write(row.getCreateTable() + "\n")

class MetricsRow(object):

    def __init__(self, tblname, schema, createTable):
        self.tablename = tblname
        self.schema = schema
        self.createTable = createTable
        
    def getTableName(self):
        return self.tablename
        
    def getSchema(self):
        return self.schema

    def getCreateTable(self):
        return self.createTable
        
    def getRow(self):
        return None
    
class FsckRow(MetricsRow):

    totalSize = 0
    totalDirs = 0
    totalFiles = 0
    totalSymlinks = 0
    totalBlocks = 0
    minRepBlocks = 0
    overRepBlocks = 0
    underRepBlocks = 0
    misRepBlocks = 0
    corruptBlocks = 0
    missReplicas = 0
    numDataNodes = 0
    numRacks = 0
     
    def __init__(self):
        tblname = "fsck"
        schema = "(size, dirs, files, symlinks, blocks, min_replicated_blocks, over_rep_blocks, under_rep_blocks, mis_rep_blocks, corrupt_blocks, missing_replicas, num_data_nodes, num_racks)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), size BIGINT, dirs BIGINT, files BIGINT, symlinks BIGINT, blocks BIGINT, min_replicated_blocks BIGINT, over_rep_blocks BIGINT, under_rep_blocks BIGINT, mis_rep_blocks BIGINT, corrupt_blocks BIGINT, missing_replicas BIGINT, num_data_nodes BIGINT, num_racks BIGINT);" % (tblname)
        super(FsckRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return (self.totalSize, self.totalDirs, self.totalFiles, self.totalSymlinks, self.totalBlocks, self.minRepBlocks, self.overRepBlocks, self.underRepBlocks, self.misRepBlocks, self.corruptBlocks, self.missReplicas, self.numDataNodes, self.numRacks)

class HdfsReportRow(MetricsRow):

    name = str()
    hostname = str()
    rack = str()
    decommission_status = str()
    conf_capacity = 0
    dfs_used = 0
    non_dfs_used = 0
    dfs_remaining = 0
    dfs_used_perc = 0
    dfs_remaining_perc = 0
    last_contact = str()
    alive = True

    def __init__(self):
        tblname = "hdfs_report"
        schema = "(name, hostname, rack, decommission_status, conf_capacity, dfs_used, non_dfs_used, dfs_remaining, dfs_used_perc, dfs_remaining_perc, last_contact, alive)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), name TEXT, hostname TEXT, rack TEXT, decommission_status TEXT, conf_capacity BIGINT, dfs_used BIGINT, non_dfs_used BIGINT, dfs_remaining BIGINT, dfs_used_perc REAL, dfs_remaining_perc REAL, last_contact TIMESTAMP, alive BOOLEAN);" % (tblname)
        super(HdfsReportRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return (self.name, self.hostname, self.rack, self.decommission_status, self.conf_capacity, self.dfs_used, self.non_dfs_used, self.dfs_remaining, self.dfs_used_perc, self.dfs_remaining_perc, self.last_contact, self.alive)

class HawqDBQuotaRow(MetricsRow):

    database = str()
    dir = str()
    quota = 0
    quotaUsed = 0
    quotaRemaining = 0 

    def __init__(self):
        tblname = "hawq_db_quotas"
        schema = "(database, dir, quota, quota_used, quota_remaining)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), database TEXT, dir TEXT, quota BIGINT, quota_used BIGINT, quota_remaining BIGINT);" % (tblname)
        super(HawqDBQuotaRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return "('%s', '%s', %s, %s, %s)" % (self.database, self.dir, "null" if self.quota is None else self.quota, "null" if self.quotaUsed is None else self.quotaUsed, "null" if self.quotaRemaining is None else self.quotaRemaining)
         
class HawqDBSizeRow(MetricsRow):

    database = str()
    size = str()

    def __init__(self):
        tblname = "hawq_db_sizes"
        schema = "(database, size)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), database TEXT, size BIGINT);" % (tblname)
        super(HawqDBSizeRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return (self.database, self.size)

class HiveDBQuotaRow(MetricsRow):

    database = str()
    dir = str()
    quota = 0
    quotaUsed = 0
    quotaRemaining = 0 

    def __init__(self):
        tblname = "hive_db_quotas"
        schema = "(database, dir, quota, quota_used, quota_remaining)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), database TEXT, dir TEXT, quota BIGINT, quota_used BIGINT, quota_remaining BIGINT);" % (tblname)
        super(HiveDBQuotaRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return "('%s', '%s', %s, %s, %s)" % (self.database, self.dir, "null" if self.quota is None else self.quota, "null" if self.quotaUsed is None else self.quotaUsed, "null" if self.quotaRemaining is None else self.quotaRemaining)
         
class HiveDBSizeRow(MetricsRow):

    database = str()
    size = str()

    def __init__(self):
        tblname = "hive_db_sizes"
        schema = "(database, size)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), database TEXT, size BIGINT);" % (tblname)
        super(HiveDBSizeRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return (self.database, self.size)
         
class UserSpaceQuotaRow(MetricsRow):

    username = str()
    dir = str()
    quota = 0
    quotaUsed = 0
    quotaRemaining = 0 

    def __init__(self):
        tblname = "user_space_quotas"
        schema = "(username, dir, quota, quota_used, quota_remaining)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), username TEXT, dir TEXT, quota BIGINT, quota_used BIGINT, quota_remaining BIGINT);" % (tblname)
        super(UserSpaceQuotaRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return "('%s', '%s', %s, %s, %s)" % (self.username, self.dir, "null" if self.quota is None else self.quota, "null" if self.quotaUsed is None else self.quotaUsed, "null" if self.quotaRemaining is None else self.quotaRemaining)

class UserINodeQuotaRow(MetricsRow):

    username = str()
    dir = str()
    quota = 0
    quotaUsed = 0
    quotaRemaining = 0 

    def __init__(self):
        tblname = "user_inode_quotas"
        schema = "(username, dir, quota, quota_used, quota_remaining)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), username TEXT, dir TEXT, quota BIGINT, quota_used BIGINT, quota_remaining BIGINT);" % (tblname)
        super(UserINodeQuotaRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return "('%s', '%s', %s, %s, %s)" % (self.username, self.dir, "null" if self.quota is None else self.quota, "null" if self.quotaUsed is None else self.quotaUsed, "null" if self.quotaRemaining is None else self.quotaRemaining)

class UserSpaceSizeRow(MetricsRow):

    username = str()
    dir = str()
    size = 0

    def __init__(self):
        tblname = "user_space_size"
        schema = "(username, dir, size)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), username TEXT, dir TEXT, size BIGINT);" % (tblname)
        super(UserSpaceSizeRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return (self.username, self.dir, self.size)
         
class UserINodeSizeRow(MetricsRow):

    username = str()
    dir = str()
    size = 0

    def __init__(self):
        tblname = "user_inode_size"
        schema = "(username, dir, size)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), username TEXT, dir TEXT, size BIGINT);" % (tblname)
        super(UserINodeSizeRow, self).__init__(tblname, schema, createTable)        
    
    def getRow(self):
         return (self.username, self.dir, self.size)

class DriveUsageRow(MetricsRow):

    host = str()
    drive = str()
    perc = 0
    size = 0
    used = 0
    avail = 0

    def __init__(self):
        tblname = "drive_usage"
        schema = "(host, drive, perc, size, used, avail)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), host TEXT, drive TEXT, perc REAL, size BIGINT, used BIGINT, avail BIGINT);" % (tblname)
        super(DriveUsageRow, self).__init__(tblname, schema, createTable)        

    def getRow(self):
         return (self.host, self.drive, self.perc, self.size, self.used, self.avail)
         
class DriveWriteTestRow(MetricsRow):

    host = str()
    drive = str()
    reason = str()

    def __init__(self):
        tblname = "drive_write_test"
        schema = "(host, drive, reason)"
        createTable = "CREATE TABLE %s (id SERIAL, ts TIMESTAMP WITH TIME ZONE DEFAULT now(), host TEXT, drive TEXT, reason TEXT);" % (tblname)
        super(DriveWriteTestRow, self).__init__(tblname, schema, createTable)        
        
    def getRow(self):
         return (self.host, self.drive, self.reason)