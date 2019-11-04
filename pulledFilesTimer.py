#!/usr/bin/env python

# pulledFilesTimer.py
#
# Dario Sciola, Jan 2019
#  
#  see 'longdescription' string which appears with help (-h) for a description of program
#
# Notes:
#  - python 2.x compatible (not 3.x)
#

import inotify.adapters
import os
import sys
import datetime
import argparse

ver       = "V1.01"

# defaults/controls  (Most/all of these can be changed via command line arguments to the program)
# ---------------------------------------------------------------------------------------------------

dir2Check     = '/home/dario/testftpout'  # directory to monitor
verbose       = False                     # verbose mode toggle. default off


# used as 'help' by argparse:
longdescription = "This script can be used to determine the time that a directory of files is 'emptied' such " + \
                  "as in the case where the files are being pulled by a move operation, rsync, s/ftp, etc. " + \
                  "The program uses inotify 'events' to trigger on the first 'read' operation signalled by " + \
                  "inotify event [IN_OPEN] at which point it begins a timer, and thereafter checks for an " + \
                  "empty directory after each inotify event. Once an empty directory is detected the timer " + \
                  "is halted and the program displays the time interval from the first Read/Open to empty " + \
                  "directory condition (Note that checking for the empty directory itself generates inotify " + \
                  "events!) " + \
                  "WARNING: Any file(s) added to the directory being monitored prior to triggering the timer "  + \
                  "will itself trigger the timer to begin."

# ------------------------------------------------------------------------------------------
#  dirEmpty(dir)
#
#  Returns True if directory is empty, False if there are one or more files.
# ------------------------------------------------------------------------------------------

def dirEmpty(dir):

    if len(os.listdir(dir) ) ==0:
        print "Directory ", dir, " is now empty!"
        return True
    else:
        return False

# ------------------------------------------------------------------------------------------
#  Main
# ------------------------------------------------------------------------------------------

def _main():

    vprint("Create adapter")
    i = inotify.adapters.Inotify()

    vprint("Adding watch for dir:", dir2Check)
    i.add_watch(dir2Check)

    startedClock = False
    
    vprint("Acquiring inode events...")

    CheckDir  = False  # flag indication we should start to check if empty dir?

    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event
        # NOTE that you get a list, type_names, not just ONE event type, but the list always
        # has one entry so use [0] to access it directly

       
        # inotify event IN_OPEN can be a trigger for a first 'read' on a pull
        if (type_names[0] == 'IN_OPEN'):
            vprint("Detected a OPEN on File ", filename)
            if not startedClock:
                startedClock = True
                CheckDir = True
                begintime = datetime.datetime.now()
                begintimestr =  (begintime.strftime("%d/%m/%Y") + ' ' + begintime.strftime("%H:%M:%S"))
                print "OPEN on file triggered timer at Timestamp:[", begintimestr , "]"

        if (type_names[0] == "IN_DELETE"):
            vprint("File ", filename, " was deleted.")

        if (type_names[0] == 'IN_MOVED_FROM'):
            vprint("File ", filename, " was moved.")

        # Check now to see if directory is empty
        # NOTE: This check itself will generate Inotify events!

        if CheckDir:
            numberOfFiles = len(os.listdir(dir2Check))  # Note: listdir does not count . and ..
            if numberOfFiles ==0:
                print 'Directory "'+ dir2Check + '" is now empty!'
                curtime = datetime.datetime.now()
                curtimestr = (curtime.strftime("%d/%m/%Y") + ' ' + curtime.strftime("%H:%M:%S"))
                proctime = (str(curtime - begintime)).split(".")[0] #truncate the fractional seconds
                print "Duration: [", proctime, "]"
                exit(2)
            else:
                vprint("Directory contains ", numberOfFiles, " left.")

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
# Main
# ------------------------------------------------------------------------------------------

if __name__ == '__main__':

    print "Running", os.path.basename(__file__), ver

    parser = argparse.ArgumentParser(epilog=longdescription)
#    Note that '-h'is automatically applied by argparse
    parser.add_argument('-v', help='verbose mode [Default: False]', action="store_true") # make it a True/False flag
    parser.add_argument('-d', help='Directory to monitor. [Default: ' + dir2Check + ']')

    args = parser.parse_args()

    # Process command line arguments (if any)

    # verbose?
    if args.v:
        verbose = True
        vprint("Verbose mode ON")

    # remote directory?
    if args.d:
        dir2Check = args.d
        vprint("Directory to monitor: " + dir2Check)


    
    # Now do some sanity checks...
 
    # First check if the directory exists

    if not os.path.isdir(dir2Check):
        print 'The directory "' + dir2Check + '" does not exist or is inaccessible. Use the -d ' +\
              'option to specify another directory to monitor.' 
        exit(1)

    # Now check to make sure there are files to monitor.

    if dirEmpty(dir2Check):
        print 'There must be files in directory "' + dir2Check + '" before you run this program!'
        exit(1)

    _main()


