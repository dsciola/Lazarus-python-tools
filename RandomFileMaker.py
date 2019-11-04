#!/usr/bin/env python
#
#
# RandomFileMaker.py
#
# Dario Sciola, March 2019
#
#   Added MD5 capability March 2019.
#  
#  see 'longdescription' string which appears with help (-h) for a description of program
#
# part the code taken from Jesse Noller:
# http://jessenoller.com/blog/2008/05/30/making-re-creatable-random-data-files-really-fast-in-python
#
# modified for multiple files and random sizes

import random
import collections
import os
import argparse
import math
import hashlib
import time

# defaults/controls  (Most/all of these can be changed via command line arguments to the program)
# ---------------------------------------------------------------------------------------------------

verbose        = False                     # verbose mode toggle. default off
NumberOfFiles  = 1                         # default number of files to generate
maxsize        = 1024 * 1024               # default maximum file size 1024 * 1024 (1Mb)
step           = maxsize -1                # step size when randomised option is used.
filenamePrefix = 'LoremFile'               # Filename prefix
doMD5          = False                     # Hash toggle.
destDir        = '.'                       # destination directory in which files deposited
workDir        = '.'                       # working directory in which files are created/appended.
waitsec        = 0                         # default time interval between files

ver = "V1.07"

PadLen = int(math.log(NumberOfFiles+1,10))+3   # we add three (not 2) to allow for the fractional log return
#PadLen =              # the length of the entire string to be created (eg "______100_" )
                       # note that the minimun required value here is a function of the 'filecnt'
                       # maximum stringified number that will be embeded in the padding.
                       # Given the equation 
                       #                                (PadLen-2)
                       #       Maximun numToEmbed =  (10                ) -1
                       #
                       # PadLen = log(filecnt+1)+2

# used as 'help' by argparse:
longdescription = "This script can be used to generate random or fixed sized text test files. Given parameters, " + \
                  "the script will generate one or more (-n) test files containing a subset or repeating sequences " + \
                  "of the typographic test staple 'Lorem Ipsum' sequence of words and paragraphs. " + \
                  "Files created have a fized prefix followed by a pad string that contains an embedded numerical " + \
                  "string that increments as files are created. The size can be specified (-m) and unless " + \
                  "a step size is provided, all files will be the same size. If a step is provided the files " + \
                  "will be randomly sized but limited to increments of the step size. " + \
                  "When using the hash option (-j), the files will have the MD5 hash prepended to the filename "  + \
                  "as well as having the pad string at the begining of the file contents (so as to ensure that "  + \
                  "a potential randon size coming up more than once still generates a unique hash on the file). "  + \
                  "If a temporary working directory is needed use -w. Using -w will create the file in the "  + \
                  "working directory and only once it is completed move it to the destination directory. "  + \
                  " " 
# The classic Lorem Ipsum text
randtext = "" + \
 "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet " + \
 "dolore magna aliquam erat volutpat. Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper suscipit " + \
 "lobortis nisl ut aliquip ex ea commodo consequat. Duis autem vel eum iriure dolor in hendrerit in vulputate velit " + \
 "esse molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et accumsan et iusto odio " + \
 "dignissim qui blandit praesent luptatum zzril delenit augue duis dolore te feugait nulla facilisi. " + \
 " \n\n" + \
 "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper suscipit lobortis nisl ut aliquip ex ea commodo " + \
 "consequat. Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse molestie consequat, vel illum dolore " + \
 "eu feugiat nulla facilisis at vero eros et accumsan et iusto odio dignissim qui blandit praesent luptatum zzril " + \
 "delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum dolor sit amet, consectetuer adipiscing elit, " + \
 "sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. " + \
 " \n\n" + \
 "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse molestie consequat, vel illum dolore eu feugiat " + \
 "nulla facilisis at vero eros et accumsan et iusto odio dignissim qui blandit praesent luptatum zzril delenit augue " + \
 "duis dolore te feugait nulla facilisi. Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy " + \
 "nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut wisi enim ad minim veniam, quis nostrud " + \
 "exerci tation ullamcorper suscipit lobortis nisl ut aliquip ex ea commodo consequat. " + \
 " \n\n"

# ------------------------------------------------------------------------------------------
# Gets MD5 of a file. Does it in chunks so as not to exhaust memory.
# ------------------------------------------------------------------------------------------

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(2 ** 20), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# ------------------------------------------------------------------------------------------
# makePad(numToEmbed)
#
# This routine will create a fixed length string which includes the number passed into the 
# routine. The intent is to create prefixed strings that can be concatenated to filenames
# or extensions when a large number of uniquely name files need to be created. The entire
# string is limited to PadLen characters, but there will be at least one padding
# character (PadChar) at the end and one at the begining.
#
# Note: The largest acceptable input is determined by the PadLen variable. Taking
#       into account one pad character at the end and one at the front:
#       
#                                (PadLen-2)
#       Maximun numToEmbed =  (10                ) -1
#
#       With a value of 10, the effective number of digits is 8, so 99,999,999
#
# ------------------------------------------------------------------------------------------

def makePad(numToEmbed):

   PadChar = "_"

   # check if being ask to embed a number too big to fit the pad length
   maxPossible = (10**(PadLen-2)) - 1

   # should not hit this but...
   if ( numToEmbed > maxPossible):
      print "Exceeding Pad string embedding limit of ", maxPossible
      exit(3)
 
   # build the front pad
   filenamePrefix = PadChar
   asciinum = str(numToEmbed);
   numpads2add = PadLen - len(asciinum) -2 # we subtract for the padding
   while ( numpads2add ):
      filenamePrefix = filenamePrefix + PadChar
      numpads2add = numpads2add -1  
      
   filenamePrefix = filenamePrefix + asciinum + PadChar

   return filenamePrefix

# ------------------------------------------------------------------------------------------
#
#
# ------------------------------------------------------------------------------------------

def add_nulls(int, cnt):
    nulls = str(int)
    for i in range(cnt - len(str(int))):
    	nulls = '0' + nulls
    return nulls

# ------------------------------------------------------------------------------------------
# fdata magic 
# ------------------------------------------------------------------------------------------

def fdata(randseed, words):
    a = collections.deque(words)
    b = collections.deque(randseed)
    while True:
        yield ' '.join(list(a)[0:1024])
        a.rotate(int(b[0]))
        b.rotate(1)


# ------------------------------------------------------------------------------------------
# main
# ------------------------------------------------------------------------------------------

def main():

#    print "In Main"

    words = randtext.split()

    for Nfile in range(NumberOfFiles):

        padstring = makePad(Nfile)
        newfile = filenamePrefix + padstring +".txt"

        if (step >= (maxsize-1) ):
            size = maxsize
        else:
            vprint("Randomizing size.")
            size = random.randrange(step, maxsize, step)

        print "Creating", newfile, "a", size, "byte file"
        
        randseed = str(random.randint(1,65535))
        g = fdata(randseed, words)

        if args.w:
           newfilepath=workDir+'/'+newfile
        else:
           newfilepath=newfile

        fh = open(newfilepath, 'w')

        # Since we want 'hash diversity' when generating a number of randon files even if some
        # (because of the use of step) end up being the same length, we'll throw in the padstring
        # (which we know is unique) into the file right away.
        if doMD5:
            fh.write(padstring)

        while os.path.getsize(newfilepath) < size:
            fh.write(g.next())
        
        fh.truncate(size)
        os.chmod(newfilepath, 0o777)
        workfile=newfilepath # save for move to dest

        if doMD5:
            hashstr=md5(newfilepath)
            print " Hash is :", hashstr
            workfile= hashstr+"-"+newfile # save for move to dest
            print "newfilepath:", newfilepath, " workfile:", workfile
            os.rename(newfilepath, workfile)

        # now just move final file from working dir to destination dir        
        os.rename(workfile, destDir+"/"+workfile)

        time.sleep(waitsec)

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
    parser.add_argument('-n', help='Number of files to generate [Default: ' + str(NumberOfFiles) + "]")
    parser.add_argument('-m', help='Default or maximum file size [Default: ' + str(maxsize) + "]")
    parser.add_argument('-s', help='Step size [Default: ' + str(step) + "]")
    parser.add_argument('-p', help='File name prefix [Default: ' +  filenamePrefix + "]")
    parser.add_argument('-j', help='Generate MD5 hash [Default: False ]', action="store_true")
    parser.add_argument('-d', help='Destination directory [Default: none ]')
    parser.add_argument('-w', help='Temporary working directory [Default: none ]')
    parser.add_argument('-x', help='Time interval (sec) between new files [Default: ' + str(waitsec) + "]")

    args = parser.parse_args()

    # verbose?
    if args.v:
        verbose = True
        vprint("Verbose mode ON")

    # Number of files?
    if args.n:
        NumberOfFiles = int(args.n)
        vprint("Number of files: " + args.n)
        PadLen = int(math.log(NumberOfFiles+1,10))+3

    # Maximum size files?
    if args.m:
        maxsize = int(args.m)
        step = maxsize-1
        vprint("Maximum size of files: " + args.m)

    # Step size?
    if args.s:
        step = int(args.s)
        if (step > maxsize-1):
            step = maxsize-1
            print "Warning: Step cannot be greater that maximun file size. Step resized to ", step
        vprint("Step size: " + str(step))

    # File name prefix?
    if args.p:
        filenamePrefix = args.p
        vprint("File name prefix: " + filenamePrefix)

    # Generate MD5?
    if args.j:
        doMD5 = True
        vprint("MD5 mode ON")

    # destination directory?
    if args.d:
        destDir = args.d
        vprint("Destination directory: " + destDir)

    # working directory?
    if args.w:
        workDir = args.w
        vprint("Working directory: " + workDir)

    # time interval?
    if args.x:
        waitsec = int(args.x)
        vprint("Time interval (sec): " + args.x)
 
    #
    #  Some quick sanity checks.
    #

    # make sure source file exists and is readable
    #if ( not os.access(loremfile, os.R_OK)) or (os.stat(loremfile).st_size == 0):
    #   print "Seed file not valid, unreadable, or empty:" + loremfile
    #   exit(2)

    # Check if the working directory exists
    # NOT COMPLETE!

    if not os.access(workDir, os.R_OK | os.W_OK  ):  #  doesn't seem to work
        print 'The working directory "' + workDir + '" does not exist or does not have Read/Write privileges!'

        try:
            os.mkdir(workDir, 0700 ) #  doesn't seem to workos.R_OK | os.W_OK here
        except OSError as exc:
            print 'Attempt to create working directory "' + workDir + '" failed. Terminating'
            exit(1)
        print 'A working directory "' + workDir + '" was created'

 
    # Check if the destination directory exists

    if not os.access(destDir, os.R_OK | os.W_OK  ):  #  doesn't seem to work
        print 'The destination directory "' + destDir + '" does not exist or does not have Read/Write privileges!'

        try:
            os.mkdir(destDir, 0700 ) #  doesn't seem to workos.R_OK | os.W_OK here
        except OSError as exc:
            print 'Attempt to create destination directory "' + destDir + '" failed. Terminating'
            exit(1)
        print 'A destination directory "' + destDir + '" was created'

    vprint("Sanity checks done.")

    main()


