
#!/usr/bin/python

import ConfigParser

from collections import namedtuple
from funcs import *

from Config import *
from PostgresUtil import *

class FsUtil:
    def __init__(self, conf):
        self.conf = conf
        self.pgUtil = PostgresUtil(conf)
        
    def printReport(self):
        printInfo("Grabbing and printing drive metrics")
        self.printDriveUsage()
        self.printDriveWriteTest()
    
    # This function prints usage statistics for drives that are available
    # It runs an initial test to make sure the drive exists on the host
    # If the drive does not exist, it is not reported in this report
    def printDriveUsage(self):
        hostfile = self.conf.get(Config.HOST_FILE)
        dataDrives = self.conf.get(Config.DATA_DRIVES)
        
        driveMetrics = []
        for dataDrive in dataDrives.split(" "):
            # First, test if the drive exists on all host, getting the list of where it worked
            output = getCommandOutput("massh %s worked test -e %s" % (hostfile, dataDrive))
            if len(output) > 0:
                # We have some hosts that have this drive
                tmpHostfile = self.writeHostFile(output.split("\n"))
                cmd = "massh %s verbose \"df %s | grep -v Filesystem\" | awk '{print $1,$8,$7,$4,$5,$6}'" % (tmpHostfile, dataDrive)
                output = getCommandOutput(cmd).split("\n")
        
                for line in output:
                    (host, drive, perc, size, used, avail) = line.split(" ")
                    driveMetrics.append((host.replace("[","").replace("]",""), drive, perc.replace("%", ""), size, used, avail))
        
        driveMetrics.sort()
        
        self.__printDriveUsageInserts(driveMetrics)
        
        row = namedtuple('Row', ['Host', 'Drive', 'PercentUsed', 'Size', 'Used', 'Avail'])

        toPrint = []
        for (host, drive, perc, size, used, avail) in driveMetrics:
            toPrint.append(row(host, drive, perc, size, used, avail))
    
        pprinttable(toPrint)
    
    def __printDriveUsageInserts(self, driveMetrics):
        for (host, drive, perc, size, used, avail) in driveMetrics:
            row = DriveUsageRow()
            row.host = host
            row.drive = drive
            row.perc = perc
            row.size = size
            row.used = used
            row.avail = avail
            
            self.pgUtil.writeInsert(row)
        
    def printDriveWriteTest(self):
        printInfo("Getting non-writeable drives")
        hostfile = self.conf.get(Config.HOST_FILE)
        dataDrives = self.conf.get(Config.DATA_DRIVES)
        
        failedDrives = []
        for drive in dataDrives.split(" "):
            # Check if the drives exist
            output = getCommandOutput("massh %s bombed sudo test -e %s" % (hostfile, drive))
            if len(output) > 0:
                for host in output.split("\n"):
                    failedDrives.append((host, drive, 'dne'))
                    
            output = getCommandOutput("massh %s worked sudo test -e %s" % (hostfile, drive))
            if len(output) > 0:
                tmpHostFile = self.writeHostFile(output.split("\n"))
                output = getCommandOutput("massh %s bombed sudo test -w %s" % (tmpHostFile, drive))
                if len(output) > 0:
                    for host in output.split("\n"):
                        failedDrives.append((host, drive, 'ro'))
                    
        if len(failedDrives) == 0:
            printInfo("No non-writeable drives to report")
        else:
            row = namedtuple('Row', ['Host', 'Drive', 'Reason'])
            
            failedDrives.sort()
            
            self.__printDriveWriteTest(failedDrives)
            
            toPrint = []
            for (host, drive, reason) in failedDrives:
                    toPrint.append(row(host, drive, reason))
        
            pprinttable(toPrint)

    def __printDriveWriteTest(self, failedDrives):
        for (host, drive, reason) in failedDrives:
            row = DriveWriteTestRow()
            row.host = host
            row.drive = drive
            row.reason = reason
            
            self.pgUtil.writeInsert(row)
        
    def writeHostFile(self, hosts):
        fName = self.conf.get(Config.TMP_DIR) + "/fsutil.txt"
        f = open(fName, 'w')

        for item in hosts:
            f.write(item + "\n")

        f.flush()
        f.close()
        return fName