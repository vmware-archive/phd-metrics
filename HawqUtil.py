
#!/usr/bin/python

import ConfigParser
import operator

from collections import namedtuple
from funcs import *

from Config import *
from HdfsUtil import *

class HawqUtil:
    def __init__(self, conf):
        self.conf = conf
        self.hdfsUtil = HdfsUtil(conf)
        self.pgUtil = PostgresUtil(conf)
        self.segDirs = self.getSegmentDirs()   
        
    def printReport(self):
        printInfo("Getting database names from HAWQ")
        dbNames = self.getDatabaseNames()
        printInfo("Getting quotas from these databases %s" % (dbNames))    
        for db in dbNames:
            self.printDatabaseQuota(db)
        self.printTopKLargestDatabases(dbNames)
        
    def getDatabaseNames(self):
        names = queryPostgres(self.conf.get(Config.HAWQ_PORT), self.conf.get(Config.HAWQ_USERNAME), self.conf.get(Config.HAWQ_METADATA_DB), "SELECT DISTINCT datname FROM pg_database", split=False)
        
        retval = []
        for name in names.split():
            if not name in self.conf.get(Config.HAWQ_SYSTEM_DB_BLACKLIST):
                retval.append(name)
        return retval
        
    def getDatabaseOID(self, name):
        oids = queryPostgres(self.conf.get(Config.HAWQ_PORT), self.conf.get(Config.HAWQ_USERNAME), self.conf.get(Config.HAWQ_METADATA_DB), "SELECT oid FROM pg_database WHERE datname = '%s'" % (name))
        if not oids is None and len(oids) == 1:
            return oids[0]
        elif oids is None:
            printError("Database %s not found" % (name))
        else:
            printError("Received %i OIDs, expecting 1" %(len(oids)))
            return None
        
    def getTableOID(self, database, table):
        oids = queryPostgres(self.conf.get(Config.HAWQ_PORT), self.conf.get(Config.HAWQ_USERNAME), database, "SELECT oid FROM pg_class WHERE relname = '%s'" % (table))
        if len(oids) == 1:
            return oids[0]
        else:
            printError("Received %i OIDs, expecting 1" %(len(oids)))
            return None
        
    def getSegmentDirs(self):
        cmd = "hdfs dfs -ls %s | grep gpseg | awk '{print $8}'" % (self.conf.get(Config.HAWQ_HDFS_DIR))
        dirs = getCommandOutput(cmd).split()
    
        if len(dirs) == 0:
            printError("Failed to get any segment directories from HDFS")
            sys.exit(1)  
            
        return dirs

    def getSchemaTables(self, database):
        output = queryPostgres(port, username, database, "SELECT table_schema, table_name FROM information_schema.tables", split=False)

        schemaTableMap = dict()
        for record in output.split('\n'):
            (schema, table) = record.strip().replace('|', '').split()
            
            if not schema in self.schemaBlackList:
                try:            
                    schemaTableMap[schema] = schemaTableMap[schema] + [table]
                except KeyError:
                    schemaTableMap[schema] = [table]

        return schemaTableMap

    def printDatabaseQuota(self, db):
        dbOID = self.getDatabaseOID(db)

        if not dbOID is None:
            printInfo("Getting quota status for database %s" % (db))
            hdfsDBDirs = []
            for segDir in self.segDirs:
                hdfsDBDirs.append("%s/16385/%s" % (segDir, dbOID))

            quotas = self.hdfsUtil.getSpaceQuotas(hdfsDBDirs)

            self.__printDBQuotaInserts(db, quotas)

            row = namedtuple('Row', ['Database', 'Directory', 'Quota', 'Remaining', 'QuotaHR', 'RemainingHR'])

            toPrint = []
            for (directory, quota, remainingQuota) in quotas:
                quotaHR = bytes2human(quota) if quota != 'none' else quota
                remainingQuotaHR = bytes2human(remainingQuota) if remainingQuota != 'inf' else remainingQuota
                toPrint.append(row(db, directory, quota, remainingQuota, quotaHR, remainingQuotaHR))
            
            pprinttable(toPrint)

    def __printDBQuotaInserts(self, db, quotas):        
        for (directory, quota, remainingQuota) in quotas:
            row = HawqDBQuotaRow()
            row.database = db
            row.dir = directory
            
            if not quota == 'none':
                row.quota = int(quota)
                row.quotaRemaining = int(remainingQuota)
                row.quotaUsed = row.quota - row.quotaRemaining
            else:
                row.quota = None
                row.quotaRemaining = None
                row.quotaUsed = None
                
            self.pgUtil.writeInsert(row)           
            
    def getDatabaseSize(self, db):
        dbOID = self.getDatabaseOID(db)

        if not dbOID is None:
            dbDir = "%s/*/16385/%s" % (self.conf.get(Config.HAWQ_HDFS_DIR), dbOID)

            sizes = self.hdfsUtil.getDirSizes([dbDir])
            
            sum = 0
            for (dir, size) in sizes:
                sum += size
            
            return (db, sum)
        else:
            return None
            
    def printTopKLargestDatabases(self, dbNames):
        k = self.conf.get(Config.REPORTER_K)
        printInfo("Getting top %s largest HAWQ databases" % (k))
        
        dbSizes = []
        for db in dbNames:
            tDbSize = self.getDatabaseSize(db)
            if not tDbSize is None:
                dbSizes.append(tDbSize)

        if len(dbSizes) == 0:
            printInfo("No HAWQ databases found in HDFS")
            return
        
        dbSizes.sort(key=operator.itemgetter(1), reverse=True)
        
        if len(dbSizes) > k:
            dbSizes = dbSizes[0:k]
            
        self.__printTopKLargestDatabasesInserts(dbSizes)
            
        # print sizes
        row = namedtuple('Row', ['Database', 'Size', 'SizeHR'])

        toPrint = []
        for (db, size) in dbSizes:
            sizeHR = bytes2human(size)
            toPrint.append(row(db, str(size), str(sizeHR)))
        pprinttable(toPrint)

    def __printTopKLargestDatabasesInserts(self, dbSizes):
    
        for (db, size) in dbSizes:
            row = HawqDBSizeRow()
            row.database = db
            row.size = size
            
            self.pgUtil.writeInsert(row)       
    
    def setDatabaseQuota(self, db, quota):
    
        if db == self.conf.get(Config.HAWQ_DB_BLACKLIST).split():
            printError("Database %s is in the blacklist. Remove to set quota" % (db))
            return
            
        dbOID = self.getDatabaseOID(db)

        if not dbOID is None:
            printInfo("Setting quota for %s to %s bytes" % (db, quota))
            hdfsDBDirs = []
            for segDir in self.segDirs:
                hdfsDBDirs.append("%s/16385/%s" % (segDir, dbOID))

            self.hdfsUtil.setSpaceQuotas(hdfsDBDirs, quota)
        else:
            sys.exit(1)

    def clearDatabaseQuota(self, db):
        dbOID = self.getDatabaseOID(db)

        if not dbOID is None:
            printInfo("Clearing quota for database %s" % (db))
            hdfsDBDirs = []
            for segDir in self.segDirs:
                hdfsDBDirs.append("%s/16385/%s" % (segDir, dbOID))

            self.hdfsUtil.clearSpaceQuotas(hdfsDBDirs)
        else:
            sys.exit(1)
