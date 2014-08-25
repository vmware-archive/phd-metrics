#!/usr/bin/python

import ConfigParser
import operator

from collections import namedtuple
from funcs import *

from Config import *
from HdfsUtil import *
from PostgresUtil import *

class UserUtil:
    def __init__(self, conf):
        self.conf = conf
        self.hdfsUtil = HdfsUtil(conf)
        self.pgUtil = PostgresUtil(conf)
        
    def printReport(self):
        self.printUserSpaceQuotas()
        self.printUserINodeQuotas()
        self.printTopKSpaceUsers()
        self.printTopKINodeUsers()
        
    def printUserSpaceQuotas(self):
        printInfo("Getting space quota status for users")

        quotas = self.hdfsUtil.getSpaceQuotas(self.getUserDirectories())
        
        if len(quotas) == 0:
            printInfo("No user directories found in HDFS")
            return
    
        quotas.sort()
        
        self.__printUserSpaceQuotasInserts(quotas)
    
        row = namedtuple('Row', ['Directory', 'Quota', 'Remaining', 'QuotaHR', 'RemainingHR'])

        toPrint = []
        for (directory, quota, remainingQuota) in quotas:
            quotaHR = bytes2human(quota) if quota != 'none' else quota
            # Sometimes the remaining quota is negative...
            if remainingQuota != 'inf':
                if long(remainingQuotaHR) < 0:
                    remainingQuotaHR = "-" + bytes2human(-long(remainingQuota))
                else:
                    remainingQuotaHR = bytes2human(remainingQuota)
            else:
                remainingQuotaHR = remainingQuota
            toPrint.append(row(directory, quota, remainingQuota, quotaHR, remainingQuotaHR))
        
        pprinttable(toPrint)
        
    def __printUserSpaceQuotasInserts(self, quotas):    
        for (directory, quota, remainingQuota) in quotas:
            row = UserSpaceQuotaRow()
            row.username = directory[6:]
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
                
    def printUserINodeQuotas(self):
        printInfo("Getting inode quota status for users")

        quotas = self.hdfsUtil.getINodeQuotas(self.getUserDirectories())
        
        if len(quotas) == 0:
            printInfo("No user directories found in HDFS")
            return
    
        quotas.sort()
        
        self.__printUserINodeQuotasInserts(quotas)
    
        row = namedtuple('Row', ['Directory', 'Quota', 'Remaining'])

        toPrint = []
        for (directory, quota, remainingQuota) in quotas:
            toPrint.append(row(directory, quota, remainingQuota))
        
        pprinttable(toPrint)
        
    def __printUserINodeQuotasInserts(self, quotas):    
        for (directory, quota, remainingQuota) in quotas:
            row = UserINodeQuotaRow()
            row.username = directory[6:]
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
            
    def printUserSpaceQuota(self, user):
        printInfo("Getting space quota status for user %s" % (user))

        quotas = self.hdfsUtil.getSpaceQuotas(["/user/%s" % (user)])
        
        if len(quotas) == 0:
            printInfo("Directory for user %s not found in HDFS" % (quotas))
            return
        
        row = namedtuple('Row', ['Directory', 'Quota', 'Remaining', 'QuotaHR', 'RemainingHR'])

        toPrint = []
        for (directory, quota, remainingQuota) in quotas:
            quotaHR = bytes2human(quota) if quota != 'none' else quota
            remainingQuotaHR = bytes2human(remainingQuota) if remainingQuota != 'inf' else remainingQuota
            toPrint.append(row(directory, quota, remainingQuota, quotaHR, remainingQuotaHR))
    
        pprinttable(toPrint)
        
    def printUserINodeQuota(self, user):
        printInfo("Getting inode quota status for user %s" % (user))

        quotas = self.hdfsUtil.getINodeQuotas(["/user/%s" % (user)])
        
        if len(quotas) == 0:
            printInfo("Directory for user %s not found in HDFS" % (quotas))
            return
    
        row = namedtuple('Row', ['Directory', 'Quota', 'Remaining'])

        toPrint = []
        for (directory, quota, remainingQuota) in quotas:
            toPrint.append(row(directory, quota, remainingQuota))
    
        pprinttable(toPrint)
         
    def printTopKSpaceUsers(self):
        k = self.conf.get(Config.REPORTER_K)
        printInfo("Getting top %s space users" % (k))
    
        sizes = self.hdfsUtil.getDirSizes(['/user'])
        
        if len(sizes) == 0:
            printInfo("No user directories found in HDFS")
            return
        
        sizes.sort(key=operator.itemgetter(1), reverse=True)
        
        if len(sizes) > k:
            sizes = sizes[0:k]
            
        self.__printTopKSpaceInserts(sizes)
            
        row = namedtuple('Row', ['User', 'Size', 'SizeHR'])

        toPrint = []
        for (dir, size) in sizes:
            sizeHR = bytes2human(size)
            toPrint.append(row(dir, str(size), str(sizeHR)))
            
        pprinttable(toPrint)
             
    def __printTopKSpaceInserts(self, sizes):    
        for (dir, size) in sizes:
            row = UserSpaceSizeRow()
            row.username = dir[6:]
            row.dir = dir
            row.size = size
            
            self.pgUtil.writeInsert(row)
            
    def printTopKINodeUsers(self):
        k = self.conf.get(Config.REPORTER_K)
        printInfo("Getting top %s inode users" % (k))
    
        counts = self.hdfsUtil.getINodeCounts(self.getUserDirectories())
        
        if len(counts) == 0:
            printInfo("No user directories found in HDFS")
            return
            
        counts.sort(key=operator.itemgetter(1), reverse=True)
        
        if len(counts) > k:
            counts = counts[0:k]
            
        self.__printTopKINodeUsersInserts(counts)
            
        row = namedtuple('Row', ['User', 'INodes'])

        toPrint = []
        for (dir, count) in counts:
            toPrint.append(row(dir, str(count)))
            
        pprinttable(toPrint)
            
    def __printTopKINodeUsersInserts(self, counts):    
        for (dir, count) in counts:
            row = UserINodeSizeRow()
            row.username = dir[6:]
            row.dir = dir
            row.size = count
            
            self.pgUtil.writeInsert(row)
            
    def setUserSpaceQuota(self, user, quota):
        if user == self.conf.get(Config.USER_DIR_BLACKLIST).split():
            printError("User %s is in the blacklist.  Remove to set quota" % (db))
            return
            
        self.hdfsUtil.setSpaceQuotas(["/user/%s" % (user)], quota)

    def clearUserSpaceQuota(self, user):
        self.hdfsUtil.clearSpaceQuotas(["/user/%s" % (user)])
        
    def setUserINodeQuota(self, user, quota):
        if user == self.conf.get(Config.USER_DIR_BLACKLIST).split():
            printError("User %s is in the blacklist.  Remove to set quota" % (db))
            return
            
        self.hdfsUtil.setINodeQuotas(["/user/%s" % (user)], quota)

    def clearUserINodeQuota(self, user):
        self.hdfsUtil.clearINodeQuotas(["/user/%s" % (user)])
        
    def getUserDirectories(self):
        return self.hdfsUtil.listDirs(['/user'])