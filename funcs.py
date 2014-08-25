#!/usr/bin/python

from optparse import OptionParser
import os.path
import subprocess
import sys
import time
import xml.etree.ElementTree as et
from collections import namedtuple

now = lambda: int(round(time.time() * 1000))

def query_yes_no(question, default="yes"):
    valid = {"yes":True,   "y":True,  "ye":True,
             "no":False,     "n":False}
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                             "(or 'y' or 'n').\n")

class bcolors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    OK = '\033[92m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    ENDC = '\033[0m'

def printInfo(msg):
    print bcolors.BLUE + msg + bcolors.ENDC

def printOK(msg):
    print bcolors.OK + msg + bcolors.ENDC

def printWarning(msg):
    print bcolors.WARNING + msg + bcolors.ENDC

def printError(msg):
    print bcolors.ERROR + msg + bcolors.ENDC

def getCommandOutput(cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(cmd, proc.returncode)
    else:
        return out.strip()

def getHosts():
    return getCommandOutput("psql -U gphdmgr -p 10432 -t -c \"SELECT DISTINCT host FROM host\"").split()

def getRoles():
    return getCommandOutput("psql -U gphdmgr -p 10432 -t -c \"SELECT role FROM role\"").split()

def doesClusterExist(cname):
    out = getCommandOutput("psql -U gphdmgr -p 10432 -t -c \"SELECT id FROM cluster WHERE cluster_name = '" + cname + "'\"")
    try:
        if int(out) > 0:
            return True
    except:
        return False

def getServiceRoles(cname, roles):
    retval=[]
    for role in roles:
        out = getCommandOutput("psql -U gphdmgr -p 10432 -t -c \"SELECT host FROM clusterhost ch, (SELECT id FROM cluster WHERE cluster_name = '" + cname + "') ids WHERE ids.id = ch.cluster_id AND role = '" + role + "'\"")
        if len(out) != 0:
            retval = retval + out.split()
    return retval

def getPropertyValue(cname, propValue):
    return getCommandOutput("psql -U gphdmgr -p 10432 -t -c \"SELECT property_value FROM clusterconfigproperty cp, (SELECT id FROM cluster WHERE cluster_name = '%s') ids WHERE ids.id = cp.cluster_id AND property_name = '%s'\"" % (cname, propValue))
        
def getConfigFiles(path):
    list = []
    getConfigFilesHelper(path, path, list)
    return list

def getConfigFilesHelper(root, path, list):
    if os.path.isfile(path):
        list.append(str(path).replace(root + "/", ""))
    else:
        for p in os.listdir(path):
            getConfigFilesHelper(root, path + "/" + p, list)
            
def queryPostgres(user, port, database, query, split=True):
    retval=[]
    out = getCommandOutput("psql -U %s -p %s %s -t -c \"%s\"" % (port, user, database, query))
    if len(out) != 0:
        if split:
            return out.split()
        else:
            return out
    else:
        return None

def pprinttable(rows):
    headers = rows[0]._fields
    lens = []
    for i in range(len(rows[0])):
        lens.append(len(max([x[i] for x in rows] + [headers[i]],key=lambda x:len(str(x)))))
    formats = []
    hformats = []
    for i in range(len(rows[0])):
        if isinstance(rows[0][i], int):
            formats.append("%%%dd" % lens[i])
        else:
            formats.append("%%-%ds" % lens[i])
        hformats.append("%%-%ds" % lens[i])
    pattern = " | ".join(formats)
    hpattern = " | ".join(hformats)
    separator = "-+-".join(['-' * n for n in lens])
    print hpattern % tuple(headers)
    print separator
    for line in rows:
        print pattern % tuple(line)
        
"""
Bytes-to-human / human-to-bytes converter.
Based on: http://goo.gl/kTQMs
Working with Python 2.x and 3.x.

Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
License: MIT
"""

# see: http://goo.gl/kTQMs
SYMBOLS = {
    'customary'     : ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext' : ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                       'zetta', 'yotta'),
    'iec'           : ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext'       : ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                       'zebi', 'yobi'),
}

def bytes2human(n, format='%(value).1f %(symbol)s', symbols='customary'):
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")
    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i+1)*10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format % locals()
    return format % dict(symbol=symbols[0], value=n)

def human2bytes(s):
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]:1}
    for i, s in enumerate(sset[1:]):
        prefix[s] = 1 << (i+1)*10
    return int(num * prefix[letter])