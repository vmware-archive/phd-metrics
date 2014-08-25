
A metrics gathering tool to assist with day-to-day operations of a Pivotal
HD cluster.  A series of reports are executed, and the output is dumped to
stdout.  You can optionally specify a --sqlfile option to write a bunch of
INSERT statements to a file to push the metrics to a postgres database.

*** The tool must run as gpadmin on the HAWQ master ***

-------

python phd-metrics.py 

Available tools:
	phd-metrics.py report
	phd-metrics.py fs-util
	phd-metrics.py hawq-util
	phd-metrics.py hdfs-util
	phd-metrics.py hive-util
	phd-metrics.py user-util
	phd-metrics.py pg-util

## Report ##

This tool executes the report action of all sub-tools.  It takes an optional
sqlfile parameter to write a series of SQL statements to.

./phd-metrics.py report --help
Usage: phd-metrics.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIGFILE, --config=CONFIGFILE
                        Configuration file (default phd-metrics.ini)
  -s SQLFILE, --sqlfile=SQLFILE
                        Filename to write SQL statements to (default none)

## FS Util ##

This tool is only used for reporting.

./phd-metrics.py fs-util --help
Usage: phd-metrics.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIGFILE, --config=CONFIGFILE
                        Configuration file (default phd-metrics.ini)
  -a ACTION, --action=ACTION
                        Choose an action: report

## HAWQ Util ##

This tool lets a user get, set, or clear database space quotas for HAWQ in
addition to generating a report.

./phd-metrics.py hawq-util --help
Usage: phd-metrics.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIGFILE, --config=CONFIGFILE
                        Configuration file (default phd-metrics.ini)
  -a ACTION, --action=ACTION
                        Choose an action: report, get, set, clear
  -d DATABASE, --database=DATABASE
                        Database to get or set (Only for get/set/clear
                        actions)
  -q QUOTA, --quota=QUOTA
                        Database quota, in bytes.  Keep in mind the 3x
                        replication. (Only for set action)

## Hive Util ##

This tool lets a user get, set, or clear database space quotas for Hive in
addition to generating a report.

./phd-metrics.py hive-util --help
Usage: phd-metrics.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIGFILE, --config=CONFIGFILE
                        Configuration file (default phd-metrics.ini)
  -a ACTION, --action=ACTION
                        Choose an action: report, get, set, clear
  -d DATABASE, --database=DATABASE
                        Database to get or set (Only for get/set actions)
  -q QUOTA, --quota=QUOTA
                        Database quota, in bytes.  Keep in mind the 3x
                        replication. (Only for set action)

## User Util ## 

This tool lets a user get, set, or clear user inode and space quotas in
addition to generating a report.

./phd-metrics.py user-util --help
Usage: phd-metrics.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIGFILE, --config=CONFIGFILE
                        Configuration file (default phd-metrics.ini)
  -a ACTION, --action=ACTION
                        Choose an action: report, get, set, clear
  -u USER, --user=USER  User name (Only for get/set /clear actions)
  -q QUOTA, --quota=QUOTA
                        User quota, in bytes.  Keep in mind the 3x
                        replication. (Only for set action)
  -t QUOTATYPE, --type=QUOTATYPE
                        The type of quota to get, set, or clear: 'inode' or
                        'space'

## Postgres Util ##

This tool is used by the other tools to write a series of insert statements.
The only end-user usage of this tool is to generate a file of CREATE TABLE
statements to initialize a database via specifying the -s option.

./phd-metrics.py pg-util --help
Usage: phd-metrics.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIGFILE, --config=CONFIGFILE
                        Configuration file (default phd-metrics.ini)
  -s SQLFILE, --sqlfile=SQLFILE
                        Filename to write SQL statements to (default none)



