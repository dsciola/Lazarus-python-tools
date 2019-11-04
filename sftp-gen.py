#!/usr/bin/env python

# sftp-gen.py
#
# Dario Sciola, Dec 2018
#  
#  see 'longdescription' string which appears with help (-h) for a description of program
#
# Notes:
#  - python 2.x compatible (not 3.x)
#  - put funtion will overide files if they already exist in the destination directory.
#  - Will crash if a file being sftp'ed is deleted prior to end of transfer being completed
#  - currently does not validate remote directory (if one is supplied)
#
# ---------------------------------------------------------------------------------------------------

import pysftp    # Note: This is basically a wrapper to Paramiko SSH (not thread safe over a connection)
import datetime
import os, sys
import time
import resource
from multiprocessing import Process, Pool
import argparse
import math

ver = "V1.04"

# defaults/controls  (Most/all of these can be changed via command line arguments to the program)
# ---------------------------------------------------------------------------------------------------

targethost    = '192.168.60.44' # sftp server host name or IP (this one is F5 Server VM)
sftpuser      = 'sftpuser'      # sftp destination userid
sftpuserpw    = 'fractal'       # sftp destination password for userid
remoteDir     = ''              # sftp session will 'cd' into this directory.
testfile      = '1MbFile.txt'   # This is the file that will be sftp'ed over and over (but with new name)
fast          = False           # Fast mode skips all loop prints and time calculations
verbose       = False           # verbose mode toggle. default off
filecnt       = 100             # total number of transfers
maxConcurrent = 10              # max number of concurrent (multiprocessing) sftp connections/sessions
                                # Get "SSH protocol banner" errors when over 10, despite higher value
                                # SSH server "MaxSessions" in sshd_config.
multiprocess  = True            # Multiprocess flag default

frontPadLen = int(math.log(filecnt+1,10))+3   # we add three (not 2) to allow for the fractional log return
#frontPadLen   =       # the length of the entire string to be created (eg "______100_" )
                       # note that the minimun required value here is a function of the 'filecnt'
                       # maximum stringified number that will be embeded in the padding.
                       # Given the equation 
                       #                                (frontPadLen-2)
                       #       Maximun numToEmbed =  (10                ) -1
                       #
                       # frontPadLen = log(filecnt+1)+2

# used as 'help' by argparse:
longdescription = "This is a simple prototype program to test sftp servers. It performs a simple " + \
                  "looping sequence of 'puts' of a sample file, appending an incremental numeric new " + \
                  "filename extension to the destination file each time. The program can be used in " + \
                  "either serial mode or multi-processing mode."

# NOTE: pysftp.Connection can have argument private_key='/path/to/keyfile'

# ------------------------------------------------------------------------------------------
# sendFileProc(sourceFile, destFile) 
#
# Initiates an pysftp connection and calls the generic mysftp sendFile function. 
#
# Note that this version is intended to be called as a Child process.
#
# ------------------------------------------------------------------------------------------

def sendFileProc(sourceFile, destFile):

    if (not fast):
        print "In sub-process. Source:", sourceFile, " Dest:", destFile

    try:
        sftp = pysftp.Connection( targethost, username=sftpuser, password=sftpuserpw, log="")
        if remoteDir:
           sftp.chdir(remoteDir) # change remote directory
        rc = sendFile(sftp, sourceFile, destFile)
        sftp.close()

    except Exception, e:
        print "Exception encountered while processing file:", destFile, " Exception=", str(e)

# ------------------------------------------------------------------------------------------
# sendFile(Connection, sourceFile, destFile) 
#
# Initiates an sftp transfer given a pre-existing pysftp Connection.
# ------------------------------------------------------------------------------------------

def sendFile(sftp, sourceFile, destFile):

    confirmed = True

    if (not fast):

        begintime = datetime.datetime.now()
        begintimestr =  (begintime.strftime("%d/%m/%Y") + ' ' + begintime.strftime("%H:%M:%S"))
        print " > Initiating transfer of file: ", destFile, "Timestamp:[", begintimestr , "]"

    rc = sftp.put(sourceFile, destFile, None, confirmed)  # upload file to public/ on remote

    if (not fast):
        curtime = datetime.datetime.now()
        curtimestr = (curtime.strftime("%d/%m/%Y") + ' ' + curtime.strftime("%H:%M:%S"))
        #proctime = (str(curtime - begintime)).split(".")[0] #truncate the fractional seconds
        proctime = (str(curtime - begintime))

        # rc will be in the format "-rw-r--r--   1 500      500      104862458 26 Nov 01:26 ?"
        # the following returns timestamp on the remote (destination) file created
        print " < Completed File ", destFile, " [", ("Remote Timestamp: {}".format(datetime.datetime.fromtimestamp(rc.st_mtime))), "] Duration:", proctime
    return rc

# ------------------------------------------------------------------------------------------
# makePadExt(numToEmbed)
#
# This routine will create a fixed length string which includes the number passed into the 
# routine. The intent is to create prefixed strings that can be concatenated to filenames
# or extensions when a large number of uniquely name files need to be created. The entire
# string is limited to frontPadLen characters, but there will be at least one padding
# character (frontPadChar) at the end and one at the begining.
#
# Note: The largest acceptable input is determined by the frontPadLen variable. Taking
#       into account one pad character at the end and one at the front:
#       
#                                (frontPadLen-2)
#       Maximun numToEmbed =  (10                ) -1
#
#       With a value of 10, the effective number of digits is 8, so 99,999,999
#
# ------------------------------------------------------------------------------------------

def makePadExt(numToEmbed):

   frontPadChar = "_"

   # check if being ask to embed a number too big to fit the pad length
   maxPossible = (10**(frontPadLen-2)) - 1

   # should not hit this but...
   if ( numToEmbed > maxPossible):
      print "Exceeding Pad string embedding limit of ", maxPossible
      exit(3)

   sys.stdout.flush()
   # build the front pad
   fname = frontPadChar
   asciinum = str(numToEmbed);
   numpads2add = frontPadLen - len(asciinum) -2 # we subtract for the padding
   while ( numpads2add ):
      fname = fname + frontPadChar
      numpads2add = numpads2add -1  
      
   fname = fname + asciinum + frontPadChar

   return fname

# ------------------------------------------------------------------------------------------
# main
# ------------------------------------------------------------------------------------------

def main():
    
    print "Current process spawn limit: ", resource.getrlimit(resource.RLIMIT_NPROC)

    print "Multi processing is ",
    if (multiprocess):

        #----------------------#
        #   Multi-processing   #
        #----------------------#

        print "ON"

        pool = Pool(processes=maxConcurrent)
        if (not fast):
            print "SFTP sessions throttled to ", maxConcurrent, " concurrent connections"
   
        for loop in range (0, filecnt):
            destfile = testfile + "." + makePadExt(loop+1)

            vprint("Starting process for ", destfile)
            if ( fast ): # THIS IS THE MINIMAL PRINT IN FAST MODE SO THAT USER KNOWS AT LEAST SOMETHING IS GOING ON!
                print str(loop)+"m" ,
                sys.stdout.flush()
            res = pool.apply_async(sendFileProc, (testfile, destfile))
        
        pool.close()
        pool.join()
       
    else:

        #----------------------#
        #  Serial processing   #
        #----------------------#

        print "OFF"

        try:
            sftp = pysftp.Connection( targethost, username=sftpuser, password=sftpuserpw, log="")
            if remoteDir:
                sftp.chdir(remoteDir) # change remote directory
            for loop in range (0, filecnt):
                destfile = testfile + "." + makePadExt(loop)
                rc = sendFile(sftp, testfile, destfile)
                if ( fast ): # THIS IS THE MINIMAL PRINT IN FAST MODE SO THAT USER KNOWS AT LEAST SOMETHING IS GOING ON!
                    print str(loop)+"s" ,
                    sys.stdout.flush()
            sftp.close()

        except Exception, e:
            print "Exception encountered while processing file:", destFile, " Exception=", str(e)


    print "All done!"


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

if __name__ == "__main__":

    print "Running", os.path.basename(__file__), ver
    parser = argparse.ArgumentParser(epilog=longdescription)
#    Note that '-h'is automatically applied by argparse
    parser.add_argument('-v', help='verbose mode [Default: False]', action="store_true") # make it a True/False flag
    parser.add_argument('-t', help='target sftp server: IP or hostname. [Default: ' + targethost + "]")
    parser.add_argument('-u', help='remote sftp username. [Default: ' + sftpuser + "]")
    parser.add_argument('-p', help='remote sftp password. [Default: ' + sftpuserpw + "]")
    parser.add_argument('-f', help='Source file to be sent (with numerical incrementing extension). [Default:  ' + testfile + "]")
    parser.add_argument('-n', help='Number of files to send [Default: ' + str(filecnt) + "]")
    parser.add_argument('-d', help='Remote directory [Default: none ]')
    parser.add_argument('-s', help='Serial-processing mode [Default: Multiprocessing]', action="store_true") # 
    parser.add_argument('-l', help='Maximum concurrent process limit [Default: ' + str(maxConcurrent) + ']')      # 
    parser.add_argument('-q', help='Quick (fast) mode. Less info than non-verbose mode. No timestamping calculations. [Default: False]', action="store_true") # make it a True/False flag

    args = parser.parse_args()

    # verbose?
    if args.v:
        verbose = True
        vprint("Verbose mode ON")

    # target Host/IP?
    if args.t:
        targethost = args.t
        vprint("Target host: ", targethost)

    # destination userid?
    if args.u:
        sftpuser = args.u
        vprint("sftp userid: " + sftpuser)

    # destination password?
    if args.p:
        sftpuserpw = args.p
        vprint("sftp password: " + sftpuserpw)

    # source file?
    if args.f:
        testfile = args.f
        vprint("Source file: " + testfile)

    # Number of files?
    if args.n:
        filecnt = int(args.n)
        vprint("Number of files: " + args.n)
        # resize the 'padding' string lenght to accomodate the actual number of files if necessary
        # note we add three (not two) because the log can return a fractional portion.
        frontPadLen = int(math.log(filecnt+1,10))+3

    # remote directory?
    if args.d:
        remoteDir = args.d
        vprint("Remote directory: " + remoteDir)

    # serial mode?
    if args.s:
        multiprocess = False
        vprint("Serial-processing mode: ON")

    # Maximum concurrent processes
    if args.l:
        maxConcurrent = int(args.l)
        vprint("Maximum concurrent process: " + args.l)

    # fast mode?
    if args.q:
        fast = True
        vprint("Fast mode: ON")

    #
    #  Some quick sanity checks.
    #

    # make sure source file exists and is readable
    if ( not os.access(testfile, os.R_OK)) or (os.stat(testfile).st_size == 0):
       print "Source file not valid, unreadable, or empty:" + testfile
       sys.exit(2)

    main()

