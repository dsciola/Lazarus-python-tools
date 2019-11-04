#!/usr/bin/env python

# ftp-gen.py
#
# Dario Sciola, Dec 2018  (Last revised May 2019)
#  
#  see 'longdescription' string which appears with help (-h)
#
# Notes:
#  BASED ON SFTP-GEN
#  - python 2.x compatible (not 3.x)
#  - put funtion will overide files if they already exist in the destination directory.
#  - Will crash if a file being ftp'ed is deleted prior to end of transfer being completed
#  - currently does not validate remote directory (if one is supplied)
# 
# TODO:
#   - add the source directory option for Multi-processing mode (no reverts to sequential)
#
#  V2:
# Program defaults to Passive FTP mode, but may still generate excessive TIME_WAIT port states.
# The OS default WAIT closures can be reduced on Linus systems by setting
#  net.ipv4.tcp_tw_recycle = 1  in the /etc/sysctl.conf file ( use "sysctl --system" to reload) 
#
#  V3:
#    - Added source directory option
#    - Active / Passive now a toggle
#
#  V4:
#  Unique connection option (when serial is used)
#
#  V5:
#  Added optional delay (-x)
#
#  V6:
#  consolidated FTP setup into function
# ---------------------------------------------------------------------------------------------------

from ftplib import FTP
import datetime
import time
import os, sys
import time
import resource
from multiprocessing import Process, Pool
import argparse
import math

ver = "V6.01"

# defaults/controls  (Most/all of these can be changed via command line arguments to the program)
# ---------------------------------------------------------------------------------------------------

targethost    = '192.168.60.44'      # ftp server host name or IP (this one is F5 Server VM)
ftpuser       = 'ftpuser'       # ftp destination userid
ftpuserpw     = 'fractal'       # ftp destination password for userid
sourcedir     = ''              # directory from which all files will be ftp'ed (overrides 'testfile') 
remoteDir     = ''              # ftp session will 'cd' into this directory.
testfile      = '1MbFile.txt'   # This is the file that will be ftp'ed over and over (but with new name)
fast          = False           # Fast mode skips all loop prints and time calculations
verbose       = False           # verbose mode toggle. default off
filecnt       = 15              # total number of transfers
maxConcurrent = 10              # max number of concurrent (multiprocessing) ftp connections/sessions
multiprocess  = True            # Multiprocess flag default
passive       = True            # passive or active ftp mode
unique        = False           # use a unique (new) connection for every file transferred.
delay         = 0               # delay between sends (seconds)
 
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
longdescription = "This is a simple prototype program to test ftp servers. It performs a simple " + \
                  "looping sequence of 'puts' of a sample file, appending an incremental numeric new " + \
                  "filename extension to the destination file each time. The program can be used in " + \
                  "either serial mode or multi-processing mode. Note that -m (which is an mput command) " + \
                  "will override -f (or the default file) and -n (or the default number of files). The " + \
                  "delay option only works in serial mode."

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
# sendFileProc(sourceFile, destFile) 
#
# Initiates an ftp connection and calls the generic ftp sendFile function. 
#
# Note that this version is intended to be called as a Child process.
#
# ------------------------------------------------------------------------------------------

def sendFileProc(sourceFile, destFile):

    if (not fast):
        vprint("In sub-process. Source:", sourceFile, " Dest:", destFile)

    try:
        ftp = FTPsetup()
        rc = sendFile(ftp, sourceFile, destFile)
        ftp.quit()

    except Exception, e:
        print "Exception encountered while processing file [Multi mode]:", destFile, " Exception=", str(e)

# ------------------------------------------------------------------------------------------
# sendFile(Connection, sourceFile, destFile) 
#
# Initiates an ftp transfer given a pre-existing ftp Connection.
# ------------------------------------------------------------------------------------------

def sendFile(ftp, sourceFile, destFile):

    confirmed = True

    if (not fast):

        begintime = datetime.datetime.now()
        begintimestr =  (begintime.strftime("%d/%m/%Y") + ' ' + begintime.strftime("%H:%M:%S"))
        print " > Initiating transfer of file: ", destFile, "Timestamp:[", begintimestr , "]"

    dafile = open(sourceFile, 'rb')
    rc = ftp.storbinary('STOR '+destFile, dafile)

    if (not fast):
        curtime = datetime.datetime.now()
        curtimestr = (curtime.strftime("%d/%m/%Y") + ' ' + curtime.strftime("%H:%M:%S"))
        #proctime = (str(curtime - begintime)).split(".")[0] #truncate the fractional seconds
        proctime = (str(curtime - begintime)) #truncate the fractional seconds

        print " < Completed File ", destFile, " [Return Code:", rc, "] Duration:", proctime

    if (delay > 0):
        time.sleep(delay)

    return rc

# ------------------------------------------------------------------------------------------
#  FTPteardown(ftpinst)
#
#  Terminates an ftp  instance
# ------------------------------------------------------------------------------------------

def FTPteardown(ftpinst):

    vprint("Terminating FTP instance")
    ftpinst.quit()

# ------------------------------------------------------------------------------------------
#  FTPsetup
#
#  Returns an active (logged in) ftp instance
# ------------------------------------------------------------------------------------------

def FTPsetup():

    try:
        ftp = FTP(targethost)
        vprint("FTP connection established.")
        vprint("Attempting FTP login. User:"+ftpuser+" Password:"+ftpuserpw)
        ftp.login(ftpuser, ftpuserpw) 
        vprint("FTP login  established.")
        if passive:
           ftp.set_pasv(True) # use active mode
           vprint("Passive mode set to True")
        else:
           ftp.set_pasv(False) # use active mode
           vprint("Passive mode set to False")
        if remoteDir:
            vprint("Attempting to change FTP directory: "+remoteDir)
            ftp.cwd(remoteDir)
            vprint("FTP directory changed: "+remoteDir)
        return ftp

    except Exception, e:
        print "Exception during FTP setup. Exception=", str(e)
        exit(3)

# ------------------------------------------------------------------------------------------
# main
# ------------------------------------------------------------------------------------------

def main():
    
    vprint("Targest host:" + targethost + "  User:" + ftpuser + "  Password:" + ftpuserpw )
    print "Current process spawn limit: ", resource.getrlimit(resource.RLIMIT_NPROC)

    print "Multi processing is ",
    if (multiprocess):

        #----------------------#
        #   Multi-processing   #
        #----------------------#

        print "ON"

        pool = Pool(processes=maxConcurrent)
        if (not fast):
            print "FTP sessions throttled to ", maxConcurrent, " concurrent connections"
   
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

        if (not unique):
            ftp = FTPsetup()


        # READY TO FTP (serial)
        # ----------------------

        if (sourcedir):
            vprint("Acquiring files from directory "+sourcedir) 
            filelist=os.listdir(sourcedir)
            filecount = len(filelist)
            if filecount == 0:
                print "Abort: No files found in source directory: " + sourcedir
                sys.exit(2)
            for file in filelist:
                 print " FILE:", file
                 try:
                     if ( unique ):
                         ftp = FTPsetup()
                         rc = sendFile(ftp, sourcedir+"/"+file, file)
                         if ( fast ): # THIS IS THE MINIMAL PRINT IN FAST MODE SO THAT USER KNOWS AT LEAST SOMETHING IS GOING ON!
                             print str(loop)+"s" ,
                             sys.stdout.flush()
                         FTPteardown(ftp)

                     if ( not unique ):
                         rc = sendFile(ftp, sourcedir+"/"+file, file)
                         if ( fast ): # THIS IS THE MINIMAL PRINT IN FAST MODE SO THAT USER KNOWS AT LEAST SOMETHING IS GOING ON!
                             print str(loop)+"s" ,
                             sys.stdout.flush()

                 except Exception, e:
                     print "Exception during FTP setup. Exception=", str(e)
                     exit(3)
            if ( not unique ):
                FTPteardown(ftp)
           
        else: # not using source dir
            try:
                for loop in range (0, filecnt):
                    destfile = testfile + "." + makePadExt(loop)

                    if ( unique ):
                        ftp = FTPsetup()

                    rc = sendFile(ftp, testfile, destfile)
                    if ( fast ): # THIS IS THE MINIMAL PRINT IN FAST MODE SO THAT USER KNOWS AT LEAST SOMETHING IS GOING ON!
                        print str(loop)+"s" ,
                        sys.stdout.flush()
                    if ( unique ):
                        FTPteardown(ftp)

                if ( not unique ):
                    FTPteardown(ftp)

            except Exception, e:
                print "Exception encountered while processing file:", destfile, " Exception=", str(e)
                exit(2)

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
    parser.add_argument('-t', help='target ftp server: IP or hostname. [Default: ' + targethost + "]")
    parser.add_argument('-u', help='remote ftp username. [Default: ' + ftpuser + "]")
    parser.add_argument('-p', help='remote ftp password. [Default: ' + ftpuserpw + "]")
    parser.add_argument('-f', help='Source file to be sent (with numerical incrementing extension). [Default:  ' + testfile + "]")
    parser.add_argument('-m', help='Directory from which to send all files. ')
    parser.add_argument('-n', help='Number of files to send [Default: ' + str(filecnt) + "]")
    parser.add_argument('-d', help='Remote directory [Default: none ]')
    parser.add_argument('-s', help='Serial-processing mode [Default: Multiprocessing]', action="store_true")
    parser.add_argument('-l', help='Maximum concurrent process limit [Default: ' + str(maxConcurrent) + ']')
    parser.add_argument('-a', help='Active mode [Default=Passive mode]', action="store_true")
    parser.add_argument('-q', help='Quick (fast) mode. Less info than non-verbose mode. No timestamping calculations. [Default: False]', action="store_true") # make it a True/False flag 
    parser.add_argument('-k', help='Use unique (new) connection for every transfer [Default: False]', action="store_true")
    parser.add_argument('-x', help='Delay (seconds) between each file [Default: ' + str(delay) + "]")

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
        ftpuser = args.u
        vprint("ftp userid: " + ftpuser)

    # destination password?
    if args.p:
        ftpuserpw = args.p
        vprint("ftp password: " + ftpuserpw)

    # source directory?
    if args.m:
        sourcedir = args.m
        vprint("Source directory: " + sourcedir)
        if args.f:
           print "Cannot use -f (file) with -m (source directory)"
           exit(1)

        if args.n:
           print "Cannot use -n (number of files) with -m (source directory)"
           exit(1)

        filesinsrc = os.listdir(sourcedir)
        if not filesinsrc:
           print "Source directory " + sourcedir +" (-m) is empty"
           exit(1)
   
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
    else: # intended to use default multi-process mode
        if args.m:     # source directory provided?
            print"Defaulting to serial mode due to use of -m (Source directory)"
            multiprocess = False

    # Maximum concurrent processes
    if args.l:
        maxConcurrent = int(args.l)
        vprint("Maximum concurrent process: " + args.l)

    # active or passive mode?
    if args.a:
        passive = False
        vprint("Active mode enabled ")

    # fast mode?
    if args.q:
        fast = True
        vprint("Fast mode: ON")

    # unique mode?
    if args.k:
        unique = True
        vprint("Unique mode: ON")

    # Number of files?
    if args.x:
        delay = int(args.x)
        vprint("Delay (sec) between files: " + args.x)
 
    #
    #  Some quick sanity checks.
    #

    # make sure source file exists and is readable
    if (not args.m):  # don't care if we're using a source directory
       if ( not os.access(testfile, os.R_OK)) or (os.stat(testfile).st_size == 0):
          print "Source file not valid, unreadable, or empty:" + testfile
          sys.exit(2)  

    main()

