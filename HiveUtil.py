#!/usr/bin/python

import ConfigParser
import operator

from collections import namedtuple
from funcs import *

from Config import *
from HdfsUtil import *
from PostgresUtil import *

class HiveUtil:
    def __init__(self, conf):
        self.conf = conf
        self.hdfsUtil = HdfsUtil(conf)
        self.pgUtil = PostgresUtil(conf)
        
    def printReport(self):
        printInfo("Fetching contents of Hive warehouse")
        
        hivedbdirs = self.getHiveDatabaseDirectories()

        self.printDatabaseQuotas(hivedbdirs)    
        self.printTopKLargestDatabases(hivedbdirs)
                
    def getHiveDatabaseDirectories(self):
        hivedirs = self.hdfsUtil.listDirs([self.conf.get(Config.HIVE_WAREHOUSE_DIR)])
        retval = []
        for dir in hivedirs:
            if dir.endswith(".db"):
                retval.append(dir)
        return retval
        
    def printDatabaseQuota(self, db):
        printInfo("Getting quota status for Hive database %s" % (db))
        
        quotas = self.hdfsUtil.getSpaceQuotas(["%s%s.db" % (self.conf.get(Config.HIVE_WAREHOUSE_DIR), db)])
    
        if len(quotas) == 0:
            printInfo("No Hive databases found")
            return;
            
        row = namedtuple('Row', ['Database', 'Directory', 'Quota', 'Remaining', 'QuotaHR', 'RemainingHR'])

        toPrint = []
        for (directory, quota, remainingQuota) in quotas:
            dbName = directory.replace(".db", "").replace(self.conf.get(Config.HIVE_WAREHOUSE_DIR), "")
            quotaHR = bytes2human(quota) if quota != 'none' else quota
            remainingQuotaHR = bytes2human(remainingQuota) if remainingQuota != 'inf' else remainingQuota
            toPrint.append(row(dbName, directory, quota, remainingQuota, quotaHR, remainingQuotaHR))
    
        pprinttable(toPrint)
        
    def printDatabaseQuotas(self, hivedbdirs):
        printInfo("Getting quota status for Hive databases")
            
        hdfsDirs = []
        for dir in hivedbdirs:
            db = self.getDbNameFromPath(dir)
            hdfsDirs.append("%s/%s.db" % (self.conf.get(Config.HIVE_WAREHOUSE_DIR), db))
        
        quotas = self.hdfsUtil.getSpaceQuotas(hdfsDirs)
        
        if len(quotas) == 0:
            printInfo("No Hive databases found")
            return;
            
        quotas.sort()
        
        self.__printDBQuotasInserts(quotas)
    
        row = namedtuple('Row', ['Database', 'Directory', 'Quota', 'Remaining', 'QuotaHR', 'RemainingHR'])

        toPrint = []
        for (directory, quota, remainingQuota) in quotas:
            dbName = directory.replace(".db", "").replace(self.conf.get(Config.HIVE_WAREHOUSE_DIR), "")
            quotaHR = bytes2human(quota) if quota != 'none' else quota
            remainingQuotaHR = bytes2human(remainingQuota) if remainingQuota != 'inf' else remainingQuota
            toPrint.append(row(dbName, directory, quota, remainingQuota, quotaHR, remainingQuotaHR))
    
        pprinttable(toPrint)
            
    def __printDBQuotasInserts(self, quotas):
        for (directory, quota, remainingQuota) in quotas:
            row = HiveDBQuotaRow()
            row.database = directory.replace(".db", "").replace(self.conf.get(Config.HIVE_WAREHOUSE_DIR), "")
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
            
    def getDatabaseSize(self, dbDir):
        sizes = self.hdfsUtil.getDirSizes([dbDir])
        
        sum = 0
        for (dir, size) in sizes:
            sum += size
        
        return (dbDir, sum)
            
    def printTopKLargestDatabases(self, hivedbdirs):
        k = self.conf.get(Config.REPORTER_K)
        printInfo("Getting top %s largest Hive databases" % (k))
        
        dbSizes = []
        for dbDir in hivedbdirs:
            tDbSize = self.getDatabaseSize(dbDir)
            if not tDbSize is None:
                dbSizes.append(tDbSize)

        if len(dbSizes) == 0:
            printInfo("No Hive databases found in HDFS")
            return
        
        dbSizes.sort(key=operator.itemgetter(1), reverse=True)
        
        if len(dbSizes) > k:
            dbSizes = dbSizes[0:k]
            
        self.__printTopKLargestDatabases(dbSizes)
            
        # print sizes
        row = namedtuple('Row', ['Database', 'Size', 'SizeHR'])

        toPrint = []
        for (db, size) in dbSizes:
            sizeHR = bytes2human(size)
            toPrint.append(row(db, str(size), str(sizeHR)))
        pprinttable(toPrint)
        
    def __printTopKLargestDatabases(self, dbSizes):
    
        for (db, size) in dbSizes:
            row = HiveDBSizeRow()
            row.database = db
            row.size = size
            
            self.pgUtil.writeInsert(row)
    
    def setDatabaseQuota(self, db, quota):
    
        if db == self.conf.get(Config.HIVE_DB_BLACKLIST).split():
            printError("Database %s is in the blacklist. Remove to set quota" % (db))
            return

        printInfo("Setting quota for %s to %s bytes" % (db, quota))
        
        self.hdfsUtil.setSpaceQuotas([self.getDbPathFromName(db)], quota)
            
    def clearDatabaseQuota(self, db):
        printInfo("Clearing quota for database %s" % (db))
        self.hdfsUtil.clearSpaceQuotas([self.getDbPathFromName(db)])
            
    def getDbNameFromPath(self, dir):
        return dir.replace(self.conf.get(Config.HIVE_WAREHOUSE_DIR), "").replace(".db", "")
        
    def getDbPathFromName(self, db):
        return "%s%s.db" % (self.conf.get(Config.HIVE_WAREHOUSE_DIR), db);
