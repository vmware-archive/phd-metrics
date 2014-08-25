#!/usr/bin/python

import ConfigParser

from collections import namedtuple
from funcs import *

from Config import *
from FsUtil import *
from HawqUtil import *
from HiveUtil import *
from HdfsUtil import *
from PostgresUtil import *
from UserUtil import *

if len(sys.argv) == 1:
    print "Available tools:"
    print "\tphd-metrics.py report"
    print "\tphd-metrics.py fs-util"
    print "\tphd-metrics.py hawq-util"
    print "\tphd-metrics.py hdfs-util"
    print "\tphd-metrics.py hive-util"
    print "\tphd-metrics.py user-util"
    print "\tphd-metrics.py pg-util"
    sys.exit(0)

if __name__ == "__main__":

# Validate tools and user
    try:
        out = getCommandOutput("which hdfs")
    except:
        printError("`which hdfs` returned a non-zero exit code.  Make sur eyou are using this utility from an HDFS node")
        sys.exit(1)
        
    if getCommandOutput("whoami") != "gpadmin":
        printError("Please execute this utility as gpadmin")
        sys.exit(2)
        
## Report option
    if sys.argv[1] == "report":
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="Configuration file (default phd-metrics.ini)", default="phd-metrics.ini")
        parser.add_option("-s", "--sqlfile", dest="sqlFile", help="Filename to write SQL statements to (default none)", default=None)
            
        conf = Config(parser, sys.argv[2:])
        
        pgutil = PostgresUtil(conf)
        pgutil.open()
        
        HdfsUtil(conf).printReport()
        HawqUtil(conf).printReport()
        HiveUtil(conf).printReport()
        UserUtil(conf).printReport()
        FsUtil(conf).printReport()
        
        pgutil.close()
        
# Local filesystem option
    elif sys.argv[1] == "fs-util":
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="Configuration file (default phd-metrics.ini)", default="phd-metrics.ini")
        parser.add_option("-a", "--action", dest="action", help="Choose an action: report", default=None)
        
        conf = Config(parser, sys.argv[2:])
        
        fsUtil = FsUtil(conf)
        
        if conf.get(Config.ACTION) == 'report':
            fsUtil.printReport();
        else:
            printError("Unknown action %s" % (conf.get(Config.ACTION)))
            
# HAWQ option
    elif sys.argv[1] == "hawq-util": 
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="Configuration file (default phd-metrics.ini)", default="phd-metrics.ini")
        parser.add_option("-a", "--action", dest="action", help="Choose an action: report, get, set, clear", default=None)
        parser.add_option("-d", "--database", dest="database", help="Database to get or set (Only for get/set/clear actions)", default=None)
        parser.add_option("-q", "--quota", dest="quota", help="Database quota, in bytes.  Keep in mind the 3x replication. (Only for set action)", default=None)        
        (options, args) = parser.parse_args(sys.argv[2:])

        conf = Config(parser, sys.argv[2:])
        
        hawqUtil = HawqUtil(conf)
        
        ### Main program

        segDirs = hawqUtil.getSegmentDirs()

        if conf.get(Config.ACTION) == 'report':
            hawqUtil.printReport();
        elif conf.get(Config.ACTION)== 'get':
            hawqUtil.printDatabaseQuota(conf.get(Config.DATABASE))

        elif conf.get(Config.ACTION) == 'set':
            try:
                quota = int(conf.get(Config.QUOTA_VALUE))
            except:
                quota = human2bytes(conf.get(Config.QUOTA_VALUE).upper())

            if query_yes_no("Are you sure you want to set the %s database's quota to %s bytes?  This could have a negative effect on this HAWQ database." % (conf.get(Config.DATABASE), quota), default="no"):
                hawqUtil.setDatabaseQuota(conf.get(Config.DATABASE), quota)
                hawqUtil.printDatabaseQuota(conf.get(Config.DATABASE))
        elif conf.get(Config.ACTION) == 'clear' and query_yes_no("Are you sure you want to clear the %s database's quota?" % (conf.get(Config.DATABASE)), default="no"):
            hawqUtil.clearDatabaseQuota(conf.get(Config.DATABASE))
            hawqUtil.printDatabaseQuota(conf.get(Config.DATABASE))
        else:
            printError("Unknown action %s" % (conf.get(Config.ACTION)))

# HDFS option
    elif sys.argv[1] == "hdfs-util": 
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="Configuration file (default phd-metrics.ini)", default="phd-metrics.ini")
        parser.add_option("-a", "--action", dest="action", help="Choose an action: report", default=None)
        (options, args) = parser.parse_args(sys.argv[2:])

        conf = Config(parser, sys.argv[2:])
        
        hdfsUtil = HdfsUtil(conf)
        
        ### Main program
        
        if conf.get(Config.ACTION) == 'report':
            hdfsUtil.printReport()
        else:
            printError("Unknown action %s" % (conf.get(Config.ACTION)))

# Hive option
    elif sys.argv[1] == "hive-util": 
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="Configuration file (default phd-metrics.ini)", default="phd-metrics.ini")
        parser.add_option("-a", "--action", dest="action", help="Choose an action: report, get, set, clear", default=None)
        parser.add_option("-d", "--database", dest="database", help="Database to get or set (Only for get/set actions)", default=None)
        parser.add_option("-q", "--quota", dest="quota", help="Database quota, in bytes.  Keep in mind the 3x replication. (Only for set action)", default=None)        
        (options, args) = parser.parse_args(sys.argv[2:])

        conf = Config(parser, sys.argv[2:])
        
        hiveUtil = HiveUtil(conf)
        
        ### Main program
        
        if conf.get(Config.ACTION) == 'report':
            hiveUtil.printReport()
        elif conf.get(Config.ACTION)== 'get':
            hiveUtil.printDatabaseQuota(conf.get(Config.DATABASE))

        elif conf.get(Config.ACTION) == 'set':
            try:
                quota = int(conf.get(Config.QUOTA_VALUE))
            except:
                quota = human2bytes(conf.get(Config.QUOTA_VALUE).upper())

            if query_yes_no("Are you sure you want to set the %s database's quota to %s bytes?  This could have a negative effect on this Hive database." % (conf.get(Config.DATABASE), quota), default="no"):
                hiveUtil.setDatabaseQuota(conf.get(Config.DATABASE), quota)
                hiveUtil.printDatabaseQuota(conf.get(Config.DATABASE))
        elif conf.get(Config.ACTION) == 'clear' and query_yes_no("Are you sure you want to clear the %s database's quota?" % (conf.get(Config.DATABASE)), default="no"):
            hiveUtil.clearDatabaseQuota(conf.get(Config.DATABASE))
            hiveUtil.printDatabaseQuota(conf.get(Config.DATABASE))
        else:
            printError("Unknown action %s" % (conf.get(Config.ACTION)))
            
# User option
    elif sys.argv[1] == "user-util": 
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="Configuration file (default phd-metrics.ini)", default="phd-metrics.ini")
        parser.add_option("-a", "--action", dest="action", help="Choose an action: report, get, set, clear", default=None)
        parser.add_option("-u", "--user", dest="user", help="User name (Only for get/set /clear actions)", default=None)
        parser.add_option("-q", "--quota", dest="quota", help="User quota, in bytes.  Keep in mind the 3x replication. (Only for set action)", default=None) 
        parser.add_option("-t", "--type", dest="quotaType", help="The type of quota to get, set, or clear: 'inode' or 'space'", default=None)    
            
        conf = Config(parser, sys.argv[2:])
        
        userUtil = UserUtil(conf)
        
        ### Main program
        
        if conf.get(Config.ACTION) == 'report':
            userUtil.printReport()
        elif conf.get(Config.ACTION)== 'get':
            if conf.get(Config.QUOTA_TYPE) == 'space':
                userUtil.printUserSpaceQuota(conf.get(Config.USER))
            elif conf.get(Config.QUOTA_TYPE) == 'inode':
                userUtil.printUserINodeQuota(conf.get(Config.USER))
        elif conf.get(Config.ACTION) == 'set':
            try:
                quota = int(conf.get(Config.QUOTA_VALUE))
            except:
                # assume this is in a human readable form if initial conversion failed
                quota = human2bytes(conf.get(Config.QUOTA_VALUE).upper())

            if conf.get(Config.QUOTA_TYPE) == 'space':
                if query_yes_no("Are you sure you want to set %s's quota to %s bytes?" % (conf.get(Config.USER), quota), default="no"):
                    userUtil.setUserSpaceQuota(conf.get(Config.USER), quota)
                    userUtil.printUserSpaceQuota(conf.get(Config.USER))
            elif conf.get(Config.QUOTA_TYPE) == 'inode':
                if query_yes_no("Are you sure you want to set %s's quota to %s inodes?" % (conf.get(Config.USER), quota), default="no"):
                    userUtil.setUserINodeQuota(conf.get(Config.USER), quota)
                    userUtil.printUserINodeQuota(conf.get(Config.USER))
        elif conf.get(Config.ACTION) == 'clear':
            if conf.get(Config.QUOTA_TYPE) == 'space' and query_yes_no("Are you sure you want to clear %s's space quota?" % (conf.get(Config.USER)), default="no"):
                userUtil.clearUserSpaceQuota(conf.get(Config.USER))
                userUtil.printUserSpaceQuota(conf.get(Config.USER))
            elif conf.get(Config.QUOTA_TYPE) == 'inode' and query_yes_no("Are you sure you want to clear %s's inode quota?" % (conf.get(Config.USER)), default="no"):
                userUtil.clearUserINodeQuota(conf.get(Config.USER))
                userUtil.printUserINodeQuota(conf.get(Config.USER))
        else:
            printError("Unknown action %s" % (conf.get(Config.ACTION)))

# postgres option
    elif sys.argv[1] == "pg-util": 
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="Configuration file (default phd-metrics.ini)", default="phd-metrics.ini")
        parser.add_option("-s", "--sqlfile", dest="sqlFile", help="Filename to write SQL statements to (default none)", default=None)
            
        conf = Config(parser, sys.argv[2:])      

        try:
            conf.get(Config.SQL_FILE)
        except KeyError:
            printError("Must specify --sqlfile option for pg-util tool")

        pgutil = PostgresUtil(conf)
        pgutil.open()
        pgutil.writeCreates()
        pgutil.close()
    else:
        printError("Unknown tool")
        
    sys.exit(0)
