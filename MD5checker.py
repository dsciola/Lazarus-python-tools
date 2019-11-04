#!/usr/bin/env python

# MD5Checker.py
#
# Dario Sciola, Mar 2019
#  
#  see 'longdescription' string which appears with help (-h) for a description of program
#
# Notes:
#  - python 2.x compatible (not 3.x)
#

import inotify.adapters
import os
import sys
import hashlib
import re
import errno
import argparse
import datetime

ver       = "V1.04"

# History
#
# V1.04 : Added 2nd directory option.


# defaults/controls  (Most/all of these can be changed via command line arguments to the program)
# ---------------------------------------------------------------------------------------------------

dir2Check    = '/'                                 # directory to monitor
workdir      = '/home/dario/python/workdir'  # working directory
dir2Check2   = '/'                                 # 2nd directory to monitor
secondDir    = False                               # Flag for 2nd directory 
verbose      = False                               # verbose mode toggle. default off
keepWhenDone = False                               # Keep files in working directory once processed?

# used as 'help' by argparse:
longdescription = "This is a dynamic script that can be used to validate the MD5 hash of files in or arriving " + \
                  "at a given directory. " + \
                  "It is designed to work with files specifically created by the RamdomFileMaker.py " + \
                  "utility invoked with the MD5 generation option (-j), but will work with any file in which " + \
                  "the linux MD5sum hash is embedded as a prefix string to the filename.  " + \
                  "At invocation the program moves any file already in the target directory to a working directory " + \
                  "and also begins to monitor the target directory for the arrival of any subsequent files " + \
                  "(as triggered by inode events [IN_CLOSE_*] or [IN_MOVED_FROM]) and thereafter moves them to " + \
                  "a working directory. All files moved to the working directory are the processsed for MD5   " + \
                  "computation, which is comparing the actual file MD5 value with the value extracted from the  " + \
                  "filename prefix. The output of the program will be a line of text for each file, designating " + \
                  "it as either GOOD, BAD, or INVALID, the latter state being those files which do not have a valid  " + \
                  "MD5 prefix in the filemane. A second directory to monitor can be invoke with -s. In this case " + \
                  "the designation lines will be preceeded by either [1] (primary) or [2] (secondary) to indicate  " + \
                  "which directories they arrived in.  " + \
                  "  " + \
                  "WARNING: The working directory should not be used for any other file I/O activity."



# ------------------------------------------------------------------------------------------
# md5(fname)
#
# Gets MD5 of a file. Does it in chunks so as not to exhaust memory.
# ------------------------------------------------------------------------------------------

def md5(fname):
    vprint("Computing hash for "+fname)
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(2 ** 20), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# ------------------------------------------------------------------------------------------
# getMD5prefix(fname)
#
# Checks a file for an MD5 string prefix in the filename. If it has a valide one,...
# The format it expects is the 32 hexadecimal character string as created by the Linux
# MD5sum command which is:
#    "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH" where H is [0-9][a-f]
# The routine will return and "INVALID" string if there was no validly formatted MD5 
# detected otherwise the MD5 string itself
# ------------------------------------------------------------------------------------------

def getMD5(fname):

    retInvalid = "INVALID"
    if (len(fname) < 33):
        vprint("REGEX check failed. File "+fname+" does not conform to MD5 prefixed filaname.")
        return retInvalid

    regexString = "^\{?[a-fA-F\d]{32}\}?$"
    matchresult =re.match(regexString,fname[:32])

    if (matchresult == None):
        vprint("REGEX check failed. File "+fname+" does not conform to MD5 prefixed filaname.")
        return retInvalid
    else:
        vprint("REGEX check passed") # result is really an <_sre.SRE_Match object>

    return fname[:32]

# ------------------------------------------------------------------------------------------
#  ProcFile(f)
#
#  This will move a file to the working directory and validate MD5.
# ------------------------------------------------------------------------------------------

def procFile(pfile):

    vprint("Processing file: "+pfile)

    # move to working directory
    destfile = workdir+"/"+pfile
    foundin2nd=False
    # if second directory enabled, we have to determine where the file ended up
    if secondDir:
        vprint("Have to check 2nd dir: "+dir2Check2)
        if os.path.isfile(dir2Check2+"/"+pfile):
            fullpath=dir2Check2+"/"+pfile
            vprint("Moving "+fullpath+" to work dir "+destfile)
            os.rename(fullpath, destfile)
            foundin2nd=True

    if not foundin2nd:
        fullpath=dir2Check+"/"+pfile
        vprint("Moving "+fullpath+" to work dir "+destfile)
        os.rename(fullpath, destfile)

    ts=dt.datetime.now() # Timestamp

    # now check for valid MD5
    extractedMD5 = getMD5(pfile)

    if secondDir:
       if foundin2nd:
           sys.stdout.write("[2]")
       else:
           sys.stdout.write("[1]")

    if (extractedMD5 == "INVALID"):
       print "[", ts, "] MD5 INVALID :", pfile
    else:
        if ( md5(destfile) ==  extractedMD5):
           print "[", ts, "] MD5 GOOD    :", pfile
        else: 
           print "[", ts, "] MD5 BAD     :", pfile

    if (not keepWhenDone):
        vprint("Deleting "+destfile)
        os.remove(destfile)

    sys.stdout.flush() 

# ------------------------------------------------------------------------------------------
#  Main
# ------------------------------------------------------------------------------------------

def _main():

    # Check to see if files already in prime directory

    numberOfFiles = len(os.listdir(dir2Check))  # Note: listdir does not count . and ..
    if numberOfFiles ==0:
        vprint("Directory "+ dir2Check + " was empty at begining")
    else:
        vprint("Directory already contains ", numberOfFiles, " file(s)")

    # Check to see if files already in prime directory
    
    if secondDir:
        numberOfFiles = len(os.listdir(dir2Check2))  # Note: listdir does not count . and ..
        if numberOfFiles ==0:
            vprint("Directory "+ dir2Check2 + " was empty at begining")
        else:
            vprint("Second directory already contains ", numberOfFiles, " file(s)")


    vprint("Create adapter")
    i = inotify.adapters.Inotify()

    vprint("Adding watch for dir:", dir2Check)
    i.add_watch(dir2Check)

    if secondDir:
        vprint("Adding watch for second dir:", dir2Check2)
        i.add_watch(dir2Check2)

    
    # Main loop
    
    while (True):    

        vprint("Acquiring inode events...")

        for event in i.event_gen(yield_nones=False):
            (_, type_names, path, filename) = event
            # NOTE that you get a list, type_names, not just ONE event type, but the list always
            # has one entry so use [0] to access it directly
       
            if (type_names[0] == "IN_CLOSE_WRITE"):
                vprint("File ", filename, " was closed (writen)")
                procFile(filename)

            if (type_names[0] == "IN_CLOSE_NOWRITE"):
                vprint("File ", filename, " was closed (not writen)")
                procFile(filename)

            if (type_names[0] == 'IN_MOVED_TO'):
               vprint("File ", filename, " was moved.")
               procFile(filename)

        # TODO:  Keypress detection for 'Q" Quit

# ------------------------------------------------------------------------------------------
# vprint(args)
#
# prints args only if verbose mode is ON
# ------------------------------------------------------------------------------------------

def vprint(*args):

    if verbose:
       for arg in args:
           print arg,
       print

# ------------------------------------------------------------------------------------------
# Main (setup)
# ------------------------------------------------------------------------------------------

if __name__ == '__main__':

    print "Running", os.path.basename(__file__), ver

    parser = argparse.ArgumentParser(epilog=longdescription)
    #    Note that '-h'is automatically applied by argparse, see epilog/longdescription
    parser.add_argument('-v', help='verbose mode [Default: False]', action="store_true") # make it a True/False flag
    parser.add_argument('-d', help='Directory to monitor. [Default: ' + dir2Check + ']')
    parser.add_argument('-w', help='Working directory capturing processed files [Default: ' + workdir + ']')
    parser.add_argument('-k', help='Keep files in working directory once processed [Default: False]', action="store_true")
    parser.add_argument('-s', help='Second directory to monitor [Default: none]')
    args = parser.parse_args()

    # Process command line arguments (if any)
    #-----------------------------

    # verbose?
    if args.v:
        verbose = True
        vprint("Verbose mode ON")

    # directory?
    if args.d:
        dir2Check = args.d
        vprint("Directory to monitor: " + dir2Check)

    # working directory?
    if args.w:
        workdir = args.w
        vprint("Working directory : " + workdir)

    # Delete file from working directory once processed?
    if args.k:
        keepWhenDone = True
        vprint("Keeping files in working directory even once processed")

    # 2nd directory?
    if args.s:
        dir2Check2 = args.s
        secondDir = True
        vprint("Second directory to monitor: " + dir2Check2)
    
    # Now do some sanity checks
    #-----------------------------
 
    # Check if the monitor directory exists

    if not os.path.isdir(dir2Check):
        print 'The target directory "' + dir2Check + '" does not exist or is inaccessible!'
        exit(1)

    # Check if the working directory exists

    if not os.access(workdir, os.R_OK | os.W_OK):
    #if not os.path.isdir(workdir):
        print 'The working directory "' + workdir + '" does not exist or does not have Read/Write privileges!'

        # TODO CREATE A WORKING DIR IF default unavailable
        try:
            os.mkdir(workdir, os.R_OK | os.W_OK)
        except OSError as exc:
            print 'Attempt to create working directory "' + workdir + '" failed. Terminating'
            exit(1)
        print 'A working directory "' + workdir + '" was created'

    # Check if second monitor directory exists

    if not os.path.isdir(dir2Check2):
        print 'The second target directory "' + dir2Check2 + '" does not exist or is inaccessible!'
        exit(1)

    dt=datetime;

    _main()


