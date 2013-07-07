#! /usr/bin/env python
from collections import deque
from copy import deepcopy
from cal import CalMessage
from cal import CalChecker
import re
import gzip
import getopt,sys
from os.path import basename
from urlparse import urlsplit
import urllib2
import shutil
import os
#,pwd
import time
import smtplib
from multiprocessing import Pool
from multiprocessing import cpu_count
from multiprocessing import current_process
from time import gmtime, strftime

#---- Main ----

def print_help():
  print "Usage:",sys.argv[0],"[options]"
	print "\n \
		-f|--file fileName mandatory if stage is specified \n \
		-s|--stage StageName \n \
		-o|--output dir to store output of the script,current dir by default.\n \
		-l|--live   live pool  \n \
		-e|--email optional email address \n \
		-u|--url    url if live is specified\n \
		-h|--help usage"
	print "*" * 150
	print "Usage:",sys.argv[0]," --live --url=\"http://cal.com/2011/06-June/02/01:00:00/logs/webscr/10/191/13/110/den1cal35/eb-sql-log_02-06-2011_00-30-55_0x0.68143304.433266.mul.gz\""
	print "				OR"
	print "Usage:",sys.argv[0],"-l --url=\"http://cal.com/2011/06-June/02/01:00:00/logs/webscr/10/191/13/110/den1cal35/eb-sql-log_02-06-2011_00-30-55_0x0.68143304.433266.mul.gz\""
	print "				OR"
	print "Usage:",sys.argv[0],"--url=\"http://cal.com/2011/06-June/02/01:00:00/logs/webscr/10/191/13/110/den1cal35/eb-sql-log_02-06-2011_00-30-55_0x0.68143304.433266.mul.gz\""
	print "				OR"
	print "Usage:",sys.argv[0],"-u \"http://cal.com/2011/06-June/02/01:00:00/logs/webscr/10/191/13/110/den1cal35/eb-sql-log_02-06-2011_00-30-55_0x0.68143304.433266.mul.gz\""
	print "				OR"
	print "Usage:",sys.argv[0],"--file=calclientlogfile.log --stage=qa_stageing_host"
	print "				OR"
	print "Usage:",sys.argv[0],"-f calclientlogfile.log -s qa_staging_host"
	print "				OR"
	print "Usage:",sys.argv[0],"--file=calclientlogfile.log"
	print "				OR"
	print "Usage:",sys.argv[0],"-f calclientlogfile.log"
	sys.exit(0)

def  on_incorrect_usage():
	print_help()

def download(url):
        """Copy the contents of a file from a given URL
        to a local file.
        """
        localFile = url.split('/')[-1]
        r = urllib2.urlopen(urllib2.Request(url))
        try:
           with open(localFile, 'wb') as f:
                   shutil.copyfileobj(r,f)
        finally:
           r.close()
	return localFile

def sendMail(body):
	sender = 'vbrugubanda@paypal.com'
	sender = 'Brugubanda, Nagasai'
	receivers=email.split(",")
	message ="From: " + sender +"\n"
	recev=";".join(str for str in receivers)
	message += "To: " + recev + "\n"
	message +="Subject: BadInstrumention Messages\n"
	message += body
	try:
	   smtpObj = smtplib.SMTP('smtp-out.companyname.com')
	   smtpObj.sendmail(sender, receivers, message)
	   print "Successfully sent email"
	except smtplib.SMTPException:
	   print "Error: unable to send email"

def ReadFile(file):
    
#Ignore a) calclient messages b) cal header messages c) $HASH lines for SQLs d) "[calmsg] ^M" lines
    ignorables=re.compile("Environment:|SQLLog|Label:|Start:|\$[0-9]+|\[calclient\]|^\[calmsg\] \r")
    reportFile=os.path.splitext(file)[0]+"-baderr.xml"
    chronq=deque([]) #To hold last two lines read
    transq=deque([]) #Read each transaction hierarchy
    idx=0
    ct=CalChecker()
#Print header (this must be run at the beginning)
    ct.printHeader()
    print "\tChecking", file , "for Bad Instrumentaion. This might take a while..."
    if(not ct.isValidcalfile(getfd(file))):
	print "sai"
	return 
    ct.checkBadInstrumentationMsg(getfd(file))
    ct.checkInvalidCharData(getfd(file))
    ct.checkUnclosedTransaction(getfd(file))
#read line by line
    for line in getfd(file):
        if (re.search(ignorables,line)):
                continue
        #Remove special chars. Find the first ":" and read the 3 chars before it for ex t06: or A22:
        line=line[line.find(":") - 3:]
        #Skip empty lines
        line=line.strip()
        if (line==""):
                continue
        idx=idx + 1
        m=CalMessage(idx,line)
	# Single line checks
	ct.checkCalMsgSize(m)
	ct.checkPayloadSendRecv(m)
	ct.checkNonZeroStatusCheckWarnErrorExceptionFatal(m)
	ct.checkTenMilliSecondsDuration(m)
	ct.checkNameFormat(m)
	ct.checkTypeFormat(m)
	ct.checkStatusFormat(m)

	#File must start with "t"
        if (idx==1):
		ct.checkFileStart(m)
        # Read a transaction including nested messages
        transq.append(m)
        if (m.isRootTransactionEnd()):
                ct.checkDuplicateName(deepcopy(transq))
                ct.checkRootType(deepcopy(transq))
                ct.checkNesting(deepcopy(transq))
                transq.clear() #Clear the transaction queue
        # Chrono check
        chronq.append(m) #Add to end of queue
        if (len(chronq)==2): #If there are two messages in the queue
                ct.checkChrono(chronq[0],chronq[1])
                chronq.popleft() #remove the first member of queue
	if (idx%1000==0):
		print "Checked", idx,"lines"	
    fd.close()
	#Print footer (this must be run at the end)
    ct.printFooter(idx)
#get the report
    ct.getReport(reportFile)
    print "Done! Report is at", reportFile
    return'%s says that %s = %s' % (
        current_process().name,file, reportFile
        )

#Open the input log file
fd=None
def getfd(file):
	global fd
	try:
	        fd=gzip.open(file)
	        fd.read(1) # read a byte to get the exception if it's not a gz file
	except IOError, e: #if not a gzip file 
	        fd=open(file) #open as regular file
	return fd
###Main###################

if __name__ == '__main__':
   print strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
   print 'cpu_count() = %d\n' % cpu_count()

   if len(sys.argv) < 2:
		print_help()
   try:
       	optlist, list = getopt.getopt(sys.argv[1:], "f:s:lo:e:u:hd:", ["file=", "stage=", "live", "output=", "email=","url=","help","dir="])
   except getopt.GetoptError:
        	print "Caught exception while parsing arguments";
	        on_incorrect_usage();
		sys.exit(0)

   hour=""
   day=""
   file=""
   live=False
   stage=""
   output="."
   email=""
   url=None
   pool=""
   dir=None
#To=pwd.getpwuid( os.getuid() )[ 0 ]
   for opt, arg in optlist:
        if opt in ("-h", "--help"):
                print_help();
        elif opt in ("-f", "--file"):
                file=arg
        elif opt in ("-s", "--stage"):
                stage=arg
        elif opt in ("-l", "--live"):
                live=True
        elif opt in ("-o", "--output"):
                output=arg
        elif opt in ("-e", "--email"):
                email=arg
        elif opt in ("-u","--url"):
		url=arg
	elif opt in ("-d","--dir"):		
		dir=os.path.abspath(arg)
	else:
		print_help()
   if (not url) and  (not file) and (not dir):
     if not (file and stage):
	if not (live and url):
		print_help();
   if url:
     try:
	"Downloading url..."
        file=download(url)
     except IOError:
       print 'Filename not found.'
#listFile=[]
   if dir:
	if os.path.isdir(dir):
		listDir=os.listdir(dir)
		print dir
		print ":".join(listDir)
		listFile=[]
		listFile=[file  for file in listDir if os.path.isfile(dir+"/"+file)]
		"""for file in listDir:
			print "sai",file,os.stat(dir+"/"+file)
			if os.path.isfile(file):
				listFile.append(file)
			else:
				print "is not af ile" """
		print listFile
		print ",".join(listFile)
		#sys.exit(-1)
	

   if output:
	try:
	  output +="/BadInstrumentation"
	  old=output + repr(time.time())#Already exits rename to old dir
	  if os.path.isdir(output): 
		  os.rename(output,old)
		  print "Renaming existing folder ......"
	 	  os.mkdir(output,0755)
	  else:
		sys.stdout.write("Creating the output dir"+output)
		os.mkdir(output,0755)
	except OSError, e:
	  	print "Error creating output directory ", output, 
		print os.error 
		sys.exit(0)
   
   
    #
    # Create pool
    #

   PROCESSES = 4
   print 'Creating pool with %d processes\n' % PROCESSES
   pool = Pool(PROCESSES)
   print 'pool = %s' % pool
   for file in listFile:
     shutil.copy(dir+"/"+file, output)
     file=output+"/"+file
     print file
   listinfile=[output+"/"+file for file in listFile]
   #imap_it = pool.imap(ReadFile,listinfile,1)
   #imap_unordered_it = pool.imap_unordered(ReadFile,listinfile,1)
#    ReadFile(file)
   print 'Ordered results using pool.imap():'
   #for x in imap_it:
    #    print '\t', x
   print 'Unordered results using pool.imap_unordered():'
   #for x in imap_unordered_it:
    #    print '\t', x
   print "Done".center(79,"-")

   print 'Ordered results using pool.map() --- will block till complete:'
   for x in pool.map(ReadFile,listinfile):
        print '\t', x
   print "Done".center(79,"-")
   print strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
   print 'cpu_count() = %d\n' % cpu_count()
   print "Done".center(79,"-")
   if email:
	body="The output is at output"
	sendMail(body)

	   
