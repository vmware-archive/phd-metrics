
#!/usr/bin/python

import ConfigParser
import re

from collections import namedtuple
from funcs import *

from Config import *
from PostgresUtil import *

class HdfsUtil:
    def __init__(self, conf):
        self.conf = conf
        self.pgutil = PostgresUtil(conf)
        
    def printReport(self):
        self.printFsckSummary()
        self.printNameNodeReport()
        
    def listDirs(self, directories):
        if len(directories) == 0:
            return []
            
        dirStr = ""
        for d in directories:
            dirStr = dirStr + "%s " %(d)
            
        cmd = "hdfs dfs -ls %s | awk '{print $8}'" % (dirStr)
        
        out = getCommandOutput(cmd)
        
        if len(out) > 0:
            return out.split("\n")
        else:
            return out
        
    def getDirSizes(self, directories):
        if len(directories) == 0:
            return []
            
        cmd = "hdfs dfs -du "
        for directory in directories:
            cmd = cmd + " " + directory
            
        cmd = cmd + " | awk '{print $1,$2}'"
    
        out = getCommandOutput(cmd)
        
        if len(out) == 0:
            return []
        else:
            retval = []
            for line in out.split('\n'):
                # Returns list of (dir, size) pairs
                retval.append((line.split(' ')[1], int(line.split(' ')[0])))
                
            return retval
            
    def printFsckSummary(self):
        printInfo("Getting FSCK summary")
        # Redirecting syslog to /dev/null
        cmd = "hdfs fsck / 2> /dev/null | grep -v \"^\.\""
        
        out = getCommandOutput(cmd)

        self.__printFsckInserts(out)
        
        print out

    def __printFsckInserts(self, lines):

        row = FsckRow()
        
        for line in lines.split("\n"):
            if "Total size" in line:
                row.totalSize = int(re.sub(r"\D", "", line))
            elif "Total dirs" in line:
                row.totalDirs = int(re.sub(r"\D", "", line))
            elif "Total files" in line:
                row.totalFiles = int(re.sub(r"\D", "", line))
            elif "Total symlinks" in line:
                row.totalSymlinks = int(re.sub(r"\D", "", line))
            elif "Total blocks" in line:
                tmp = line.split('\t')[1]
                row.totalBlocks = int(tmp[0:tmp.index(' ')])
            elif "Minimally replicated blocks" in line:
                tmp = line.split('\t')[1]
                row.minRepBlocks = int(tmp[0:tmp.index(' ')])
            elif "Over-replicated blocks" in line:
                tmp = line.split('\t')[1]
                row.overRepBlocks = int(tmp[0:tmp.index(' ')])
            elif "Under-replicated blocks" in line:
                tmp = line.split('\t')[1]
                row.underRepBlocks = int(tmp[0:tmp.index(' ')])
            elif "Mis-replicated blocks" in line:
                tmp = line.split('\t')[2]
                row.misRepBlocks = int(tmp[0:tmp.index(' ')])
            elif "Corrupt blocks" in line:
                row.corruptBlocks = int(re.sub(r"\D", "", line))
            elif "Missing replicas" in line:
                tmp = line.split('\t')[2]
                row.missReplicas = int(tmp[0:tmp.index(' ')])
            elif "Number of data-nodes" in line:
                row.numDataNodes = int(re.sub(r"\D", "", line))
            elif "Number of racks" in line:
                row.numRacks = int(re.sub(r"\D", "", line))
        
        self.pgutil.writeInsert(row)
        
    def printNameNodeReport(self):
        printInfo("Getting NameNode report")
        # Redirecting syslog to /dev/null
        cmd = "hdfs dfsadmin -report 2> /dev/null | grep -v \"^\.\""
        out = getCommandOutput(cmd)
        
        self.__printNameNodeReportInserts(out)

        print out

    def __printNameNodeReportInserts(self, lines):

        row = None
        alive = True
        hitLive = False
        for line in lines.split("\n"):
            if "Live datanodes:" in line:
                alive = True
                hitLive = True
            elif "Dead datanodes:" in line:
                alive = False

            if not hitLive:
                continue

            if "Name:" in line:
                # Write out the  to our list if we've hit a new node report
                if not row is None:
                    self.pgutil.writeInsert(row)
                # make a new row 
                row = HdfsReportRow()
                row.name = line[line.index(' ')+1:line.index('(')-1]
                row.alive = alive
            elif "Hostname:" in line:
                row.hostname = line[line.index(' ')+1:]
            elif "Rack:" in line:
                row.rack = line[line.index(' ')+1:]
            elif "Decommission Status :" in line:
                row.decommission_status = line.split(' ')[3]
            elif "Configured Capacity:" in line:
                row.conf_capacity = int(line.split(' ')[2])
            elif "DFS Used:" in line[0:9]:
                row.dfs_used = int(line.split(' ')[2])
            elif "Non DFS Used:" in line:
                row.non_dfs_used = int(line.split(' ')[3])
            elif "DFS Remaining:" in line:
                row.dfs_remaining = int(line.split(' ')[2])
            elif "DFS Used%:" in line:
                row.dfs_used_perc = float(line.split(' ')[2][0:len(line.split(' ')[2])-1])
            elif "DFS Remaining%:" in line:
                row.dfs_remaining_perc = float(line.split(' ')[2][0:len(line.split(' ')[2])-1])
            elif "Last contact:" in line:
                row.last_contact = line[14:]
    
        # Write out the last row    
        if not row is None:
            self.pgutil.writeInsert(row)
        
    def getINodeCounts(self, directories):
        if len(directories) == 0:
            return []
            
        retval = []
        for directory in directories:
            # Redirecting syslog to /dev/null
            cmd = "hdfs fsck %s 2> /dev/null | grep Total | egrep \"Total dirs|Total files|Total blocks\"" % (directory)
            
            iNodeCount = 0
            for line in getCommandOutput(cmd).split('\n'):
                if 'dirs' in line:
                    iNodeCount += int(line.split('\t')[1])
                if 'files' in line:
                    iNodeCount += int(line.split('\t')[1])
                if 'blocks' in line:
                    iNodeCount += int(line.split('\t')[1][0:1])
            
            retval.append((directory, iNodeCount))
            
        return retval

    def getSpaceQuotas(self, directories):
        if len(directories) == 0:
            return []
            
        cmd = "hdfs dfs -count -q"
        for directory in directories:
            cmd = cmd + " " + directory
            
        try:
            quotas = getCommandOutput(cmd).split("\n")
        except subprocess.CalledProcessError:
            printError("Directories not found: %s" % (cmd))
            sys.exit(1)

        retval = []
        for quota in quotas:
            # Returns list of (directory, quota, remainingQuota)
            retval.append(( quota.split()[7], quota.split()[2], quota.split()[3] ))
        return retval
        
    def setSpaceQuotas(self, directories, quota):
        if len(directories) == 0:
            return []
            
        cmd = "hdfs dfsadmin -setSpaceQuota %s" % (quota)
        for directory in directories:
            cmd = cmd + " " + directory

        try:
            getCommandOutput(cmd)
        except subprocess.CalledProcessError:
            printError("Directories not found: %s" % (cmd))
            sys.exit(1)

    def clearSpaceQuotas(self, directories):
        if len(directories) == 0:
            return []

        cmd = "hdfs dfsadmin -clrSpaceQuota"
        for directory in directories:
            cmd = cmd + " " + directory
        
        try:
            getCommandOutput(cmd)
        except subprocess.CalledProcessError:
            printError("Directories not found: %s" % (cmd))
            sys.exit(1)
        
    def getINodeQuotas(self, directories):    
        if len(directories) == 0:
            return []
            
        cmd = "hdfs dfs -count -q"
        for directory in directories:
            cmd = cmd + " " + directory

        try:
            quotas = getCommandOutput(cmd).split("\n")
        except subprocess.CalledProcessError:
            printError("Directories not found: %s" % (cmd))
            sys.exit(1)

        retval = []
        for quota in quotas:
            # TODO get the proper indexes from the count
            retval.append(( quota.split()[7], quota.split()[0], quota.split()[1] ))
        return retval
        
    def setINodeQuotas(self, directories, quota):    
        if len(directories) == 0:
            return []

        cmd = "hdfs dfsadmin -setQuota %s" % (quota)
        for directory in directories:
            cmd = cmd + " " + directory
            
        try:
            getCommandOutput(cmd).split("\n")
        except subprocess.CalledProcessError:
            printError("Directories not found: %s" % (cmd))
            sys.exit(1)

    def clearINodeQuotas(self, directories):    
        if len(directories) == 0:
            return []

        cmd = "hdfs dfsadmin -clrQuota"
        for directory in directories:
            cmd = cmd + " " + directory

        try:
            getCommandOutput(cmd).split("\n")
        except subprocess.CalledProcessError:
            printError("Directories not found: %s" % (cmd))
            sys.exit(1)
