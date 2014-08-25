#!/usr/bin/python

import ConfigParser

from collections import namedtuple
from funcs import *

# This class stores configuration parameters from a file or via command line
class Config:

    # Command line params
    QUOTA_VALUE = "quota"
    QUOTA_TYPE = "type"
    CONFIG_FILE = "config"
    ACTION = "action"
    DATABASE="database"
    USER="user"
    SQL_FILE = "sqlfile"
    
    # File params
    REPORTER_K = "reporter.k"
    HOST_FILE = "host.file"
    DATA_DRIVES = "data.drives"
    TMP_DIR = "tmp.dir" 
    
    HAWQ_HOST = "hawq.host"
    HAWQ_PORT = "hawq.port"
    HAWQ_USERNAME = "hawq.username"
    HAWQ_METADATA_DB = "hawq.metadata.db"
    HAWQ_PASSWORD = "hawq.password"
    HAWQ_SYSTEM_SCHEMA_BLACKLIST = "hawq.system.schema.blacklist"
    HAWQ_SYSTEM_DB_BLACKLIST = "hawq.system.db.blacklist"
    HAWQ_DB_BLACKLIST = "hawq.db.blacklist"
    HAWQ_HDFS_DIR = "hawq.hdfs.dir"
    
    HIVE_DB_BLACKLIST = "hive.db.blacklist"
    USER_DIR_BLACKLIST = "user.dir.blacklist"
    
    HIVE_WAREHOUSE_DIR = "hive.warehouse.dir"
    
    def __init__(self, parser, args):
        (options, args) = parser.parse_args(args)
        
        self.conf = {}
        
        # Parse configuration from command line
        self.conf[Config.CONFIG_FILE] = options.configFile
    
        if parser.has_option("--" + Config.QUOTA_VALUE) and not options.quota is None:
            self.conf[Config.QUOTA_VALUE] = options.quota
        
        if parser.has_option("--" + Config.ACTION):
            if options.action is None:
                printError("Please specify a valid action with -a")
                parser.print_help()
                sys.exit(1)
            else:
                self.conf[Config.ACTION] = options.action
        
        if parser.has_option("--" + Config.DATABASE):
            self.conf[Config.DATABASE] = options.database
            hasDb = True
        else:
            hasDb = False
            
        if parser.has_option("--" + Config.USER):
            self.conf[Config.USER] = options.user
            if (options.action == 'get' or options.action == 'set' or options.action == 'clear'):
                if not options.quotaType is None:
                    if options.quotaType == 'inode' or options.quotaType == 'space':
                        self.conf[Config.QUOTA_TYPE] = options.quotaType
                    else:
                        printError("Unknown quota type %s" % (self.conf.get(Config.QUOTA_TYPE)))
                        parser.print_help()
                        sys.exit(1)
                else:
                    printError("Must specify a quota type for get, set, or clear action")
                    parser.print_help()
                    sys.exit(1)
    
        if parser.has_option("--" + Config.ACTION):
            if parser.has_option("--" + Config.DATABASE) and (options.action == 'get' or options.action == 'set' or options.action == 'clear') and (options.database is None):
                printError("Must specify a database with get, set, or clear action")
                parser.print_help()
                sys.exit(1)
            if parser.has_option("--" + Config.USER) and (options.action == 'get' or options.action == 'set' or options.action == 'clear') and (options.user is None):
                    printError("Must specify a user with get, set, or clear action")
                    parser.print_help()
                    sys.exit(1)
            if options.action == 'set' and options.quota is None:
                    printError("Must specify -q with set action")
                    parser.print_help()
                    sys.exit(1)
                    
        if parser.has_option("--" + Config.SQL_FILE) and not options.sqlFile is None:
            self.conf[Config.SQL_FILE] = options.sqlFile
            
        # Parse configuration from file
        config = ConfigParser.ConfigParser()
        config.read(self.conf[Config.CONFIG_FILE])

        params = config.options("config")
        for param in params:
            try:
                self.conf[param] = config.get("config", param)
            except :
                print("exception on %s!" % param)
                self.conf[param] = None
                
        # convert K to int
        self.conf[Config.REPORTER_K] = int(self.conf[Config.REPORTER_K])
        
    def get(self, name):
        return self.conf[name]
        
    def __str__(self):
        return "%s" % (self.conf)
