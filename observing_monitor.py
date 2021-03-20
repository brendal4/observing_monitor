#! /software/lsstsw/stack_20200515/python/miniconda3-4.7.12/envs/lsst-scipipe/bin/python
import configargparse
from datetime import datetime, timedelta, timezone
import time
import sqlite3
import sys
import os
import glob
import numpy as np
import pandas as pd
import re
import sqlalchemy
import statistics
import traceback
import psycopg2
from webpage import db_to_html

def getnite(indate):
    if len(indate) == 8:
        if indate.isdigit():
            return indate[:4]+'-'+indate[4:6]+'-'+indate[6:8]
    if len(indate) == 10:
        if  indate[:4].isdigit() & indate[5:7].isdigit() & indate[8:10].isdigit():
            return indate[:4]+'-'+indate[5:7]+'-'+indate[8:10]
    return ''

def findtable(DIR):
    DIRL=DIR.lower()
    if "ncsa_auxtel" in DIRL:
        return "nts_auxtel_"
    if "ncsa_comcam" in DIRL:
        return "nts_comcam_"
    if "auxtel" in DIRL:
        if "archiver" in DIRL:
            return "obs_auxtel_arc_"
        if "daq" in DIRL:
            return "obs_auxtel_ccs_"

    if "auxtel" in DIRL:
        if "archiver" in DIRL:
            return "obs_comcam_arc_"
        if "ccs" in DIRL:
            return "obs_comcam_ccs_"
    if "bot" in DIRL:
        return "slac_bot_"
    print("Could not determine database for directoy "+DIR+". Exiting")
    sys.exit(1)

def countdays(num_days, first_day,last_day):
    if first_day is None:
       if num_days is not None:
          return num_days
       else:
          return 2
    else:
       first_day_stamp=datetime(int(first_day[0:4]),int(first_day[4:6]),int(first_day[6:8]),0,tzinfo=timezone.utc).timestamp()
    if last_day is None:
       last_day_stamp=(datetime.utcnow()).timestamp()
    else:
       last_day_stamp=datetime(int(last_day[0:4]),int(last_day[4:6]),int(last_day[6:8]),0,tzinfo=timezone.utc).timestamp()
    if first_day_stamp > last_day_stamp:
       print("First day is after last day. Exiting.")
       sys.exit(1)
    return int((last_day_stamp-first_day_stamp)/3600/24+1)


def findpath(file1,storage_dir):
    for file2 in glob.glob(storage_dir+'/*/*.fits'):
      if samelink(file1,file2):
        return(file2)
    return 'None'

def timetonite(time):
    return (datetime.utcfromtimestamp(time)-timedelta(hours=12)).strftime('%Y-%m-%d')

def rec_listdir(dir):
    output=[]
    filelist=os.listdir(dir)
    for file in filelist:
        if os.path.isdir(dir+'/'+file):
           for file2 in os.listdir(dir+'/'+file):
               if file2[0] != '.':
                  output.append(file+'/'+file2)
        else:
           if file[0] != '.':
              output.append(file)
    return sorted(output)

def trimslash(DIRLIST):
    output = []
    for DIR in DIRLIST:
        if DIR[-1] == '/':
           output.append(DIR[:-1])
        else:
           output.append(DIR)
    return output

def dirisnite(dir):
    if not dir[:4].isdigit():
      return False
    if not dir[-2:].isdigit():
      return False
    if len(dir) == 10:
      if not (dir[4] == '-' and dir[7] == '-'):
        return False
      if dir[5:7].isdigit():
        return True
    if len(dir) == 8:
      if dir[4:6].isdigit():
        return True
    return False
    
def samelink(file1,file2):
    s1 = os.stat(file1)
    s2 = os.stat(file2)
    return (s1.st_ino, s1.st_dev) == (s2.st_ino, s2.st_dev)

def get_config():
    """Parse command line args, config file, environment variables to get configuration.
    Returns
    -------
    config: `dict`
        Configuration values for program.
    """
    parser = configargparse.ArgParser(config_file_parser_class=configargparse.YAMLConfigFileParser,
                                      auto_env_var_prefix="RSYMON_")
    parser.add('--input_dir', action='store', type=str, required=True,
               help='Path to input directory at NCSA. Must include storage and gen2repo subdirectories')
    parser.add('--first_day', action='store', type=str, required=False,
               help='Date in YYYYMMYY format for first. Must be before last day. Will override num_days')
    parser.add('--gen', action='store', type=int, required=False, default=2,
               help='Generation butler (2 or 3) supported.')
    parser.add('--last_day', action='store', type=str, required=False,
               help='Date in YYYYMMYY format for last day if not today.')
    parser.add('--ingest_log', action='store', type=str, required=False,
               help="Location of ingest log (will default to today's log.")
    parser.add('--output_dir', action='store', type=str, required=True,
               help='Path to output directory where DB and webpage will live')
    parser.add('--search_links', action='store_true', 
               help='Search repo for all links (for repos with nonstandard db format).')
    parser.add('--num_days', action='store', type=int, required=False,
               help='Number of days before the last date.')
    config = vars(parser.parse_args())
    return config

class db_filler:
    def __init__(self,input_dir,output_dir,ingest_log,gen,last_day=[]):
        self.now=datetime.utcnow()
        self.nowstr=self.now.strftime('%Y-%m-%dT%H:%M:%S')
        self.input_dir=input_dir
        self.output_dir=output_dir
        self.lock=output_dir+'/.monitor.lock'
        self.name=output_dir.split('/')[-1]
        self.gen=gen
        if len(self.name) == 0:
           self.name=output_dir.split('/')[-2]
        self.check_input_dir()
        self.check_output_dir()
        self.check_lock()
        if last_day is None:
           self.last_day=self.now
        else:
           self.last_day=datetime(int(last_day[0:4]),int(last_day[4:6]),int(last_day[6:8]),0,tzinfo=timezone.utc)
        self.ingest_log = ingest_log
    def db_conn(self):
        params=config()
        conn = psycopg2.connect(**params)
    def check_input_dir(self):
        self.repo_dir=self.input_dir+'/gen2repo/'
        self.raw_dir=self.repo_dir+'raw/'
        self.repo=self.repo_dir+'registry.sqlite3'
        self.storage=self.input_dir+'/storage/'
        if not os.path.exists(self.input_dir):
          print(self.input_dir+" does not exist. Exiting.")
          sys.exit(1)
        if not os.path.exists(self.repo):
          print(self.repo+" does not exist. Exiting.")
          sys.exit(1)
        if not os.path.exists(self.storage):
          print(self.storage+" does not exist. Exiting.")
          sys.exit(1)

    def check_output_dir(self):
        self.html=self.output_dir+'/index.html'
        self.db=self.output_dir+'/observing_monitor.sqlite3'  
        os.makedirs(self.output_dir,exist_ok=True)
        if not os.path.exists(self.output_dir):
          print("You do not have access to "+self.output_dir+". Exiting.")
          sys.exit(1)
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS FILE_COUNT (Nite_Obs TEXT PRIMARY KEY, Last_Update TEXT, N_Files INTEGER, Last_Creation TEXT, N_Ingest INTEGER, N_Small INTEGER, N_Not_Fits, N_Error INTEGER, Last_Ingest TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS FILE_COUNT_GEN3 (Nite_Obs TEXT PRIMARY KEY, Last_Update TEXT, N_Files INTEGER, Last_Creation TEXT, N_Ingest INTEGER, N_Small INTEGER, N_Not_Fits, N_Error INTEGER, Last_Transfer TEXT, Last_Ingest TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS FILE_LIST_GEN3 (Filenum INTEGER PRIMARY KEY, Nite TEXT, Last_Update TEXT, Transfer_Path TEXT, Status TEXT, Creation_Time TEXT, Transfer_Time TEXT, Ingest_Time TEXT, File_Size INT, Err_Message TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS TRANSFER_LIST (Filenum INTEGER PRIMARY KEY,  Nite_Trans TEXT, Last_Update TEXT, Transfer_Path TEXT, Creation_Time TEXT, File_Size INT)")
        c.execute("CREATE TABLE IF NOT EXISTS INGEST_LIST (FILENUM INTEGER PRIMARY KEY, Ingest_Path TEXT, Nite_Obs TEXT, Last_Update TEXT, Ingest_Time TEXT)")
        conn.commit()
        conn.close()

    def check_lock(self):
        if os.path.exists(self.lock):
          print("The lock file, "+self.lock+" exists. This indicates that another process is running. Delete it if you think this is an error. Exiting.")
          sys.exit(1)
        open(self.lock, 'a').close() 

    def set_date(self,num):
        self.nite=(self.last_day-timedelta(days=num)).strftime('%Y-%m-%d')
        self.nite_no_hyphen=(self.last_day-timedelta(days=num)).strftime('%Y%m%d')
        self.next_nite=(self.last_day-timedelta(days=num-1)).strftime('%Y-%m-%d')
        self.next_nite_no_hyphen=(self.last_day-timedelta(days=num-1)).strftime('%Y%m%d')
        mintime=datetime(int(self.nite[0:4]),int(self.nite[5:7]),int(self.nite[8:10]),12, tzinfo=timezone.utc)
        self.mintime=mintime.timestamp()
        self.maxtime=(mintime+timedelta(days=1)).timestamp()

    def count_new_files(self,DIRLIST=[]):
        self.nfiles=0
        self.filenames=[]
        self.tdevs=[]
        self.tinos=[]
        self.tpaths=[]
        self.ttimes=[]
        self.ttimestrs=[]
        self.tnites=[]
        self.filesizes=[]
        if DIRLIST == []:
#            DIRLIST=[self.nite, self.nite_no_hyphen, self.next_nite, self.next_nite_no_hyphen]
            DIRLIST=[self.nite, self.nite_no_hyphen]
        for DIR in DIRLIST:
            if DIR in [self.next_nite, self.next_nite_no_hyphen]:
                nite=self.next_nite
            else:
                nite=self.nite
            dirfile=0
            if os.path.exists(self.storage+DIR):
                 filelist=[DIR+'/'+fn for fn in sorted(rec_listdir(self.storage+DIR))]
                 for FILE in filelist:
                     PATH=self.storage+FILE
                     ttime=os.path.getmtime(PATH)
                     STAT = os.stat(PATH)
                     self.filenames.append(FILE)
                     self.tpaths.append(PATH)
                     self.tdevs.append(STAT.st_dev)
                     self.tinos.append(STAT.st_ino)
                     self.filesizes.append(STAT.st_size)
                     self.ttimes.append(ttime)
                     self.ttimestrs.append(datetime.utcfromtimestamp(ttime).strftime('%Y-%m-%dT%H:%M:%S'))
                     self.tnites.append(nite)
                     self.nfiles += 1
                     dirfile+=1

    def count_files_gen3(self):   
        table=findtable(self.input_dir) 
        query="set TIMEZONE='UTC'; with ne as (select id, relpath||'/'||filename as path, status, added_on, start_time, err_message from "+table+"files, "+table+"gen3_file_events where files_id = id and added_on > '"+self.nite+" 00:00:00+00'),  max as (select id as mid, max(start_time) as mtime from ne group by id) select id, path, status, CAST(CAST(added_on as timestamp) as varchar(25))  as ttime, CAST(CAST(start_time as timestamp) as varchar(25))  as itime, err_message from ne, max where id = mid and mtime = start_time"
        engine = sqlalchemy.create_engine('postgresql://svclsstdbdbbbm@lsst-pg-prod1.ncsa.illinois.edu:5432/lsstdb1')
        df=pd.read_sql(query, engine)
        self.paths=np.array(df['path'])
        self.ids=np.array(df['id'],dtype=int)
        self.nites=np.array(len(df)*['0000-00-00'])
        self.statuss=np.array(df['status'])
        self.ctimes=np.array(len(df)*['0000-00-00T00:00:00.00'])
        self.ttimes=np.array(df['ttime'])
        self.itimes=np.array(df['itime'])
        self.filesizes=np.zeros(len(df),dtype=int)
        self.err_messages=np.array(df['err_message'])
        self.err_messages[self.err_messages==None]=''
        for num in range(len(df)):
            FULLPATH = self.storage+self.paths[num]
            self.ttimes[num] = self.ttimes[num].replace(' ','T')
            self.itimes[num] = self.itimes[num].replace(' ','T')
            self.nites[num] = (datetime.strptime(self.ttimes[num],'%Y-%m-%dT%H:%M:%S.%f')-timedelta(hours=12)).strftime('%Y-%m-%d')
            if os.path.exists(FULLPATH):
               self.filesizes[num]=(os.stat(FULLPATH).st_size)
               self.ctimes[num]=datetime.utcfromtimestamp(os.lstat(FULLPATH).st_ctime).strftime('%Y-%m-%dT%H:%M:%S.%f')
               if self.ctimes[num] < self.ttimes[num]:
                   self.nites[num]=(datetime.strptime(self.ctimes[num],'%Y-%m-%dT%H:%M:%S.%f')-timedelta(hours=12)).strftime('%Y-%m-%d')
            else:
               print(FULLPATH+" not found.")
            nite = getnite(self.paths[num].split('/')[0])
            if nite != '':
               self.nites[num] = nite 
  
    def count_links(self):    
        self.ltimes=[]
        self.ltimestrs=[]
        self.ltimetttt=[]
        self.lpaths=[]
        self.ldevs=[]
        self.linos=[]
        self.lfilepaths=[]
        self.lnites=[]
        self.nlinks=0
        conn = sqlite3.connect(self.repo)
        c = conn.cursor()
        query='PRAGMA table_info(raw)'
        c.execute(query)
        columns= [element[1].lower() for element in c.fetchall()]
# Does this table have raftname (BOT, comcam) or not (auxtel)?
        if 'raftname' in columns:        
           query="select dayObs||'/'||expId||'/'||expid||'-'|| raftname ||'-'|| detectorName||'-det'||printf('%03d', detector)||'.fits' as FILENAME  from raw where dayObs = '"+self.nite+"' order by FILENAME"
        else:
           query="select dayObs||'/'||expid||'-det'||printf('%03d', detector)||'.fits' as FILENAME from raw where dayObs = '"+self.nite+"' order by FILENAME"
           
        c.execute(query)
        rows=c.fetchall()
        conn.close()
        for FILENAME in [row[0] for row in rows]:
            FULLPATH = self.raw_dir+FILENAME
            if os.path.exists(FULLPATH):
                self.ltimes.append(os.lstat(FULLPATH).st_atime)
                self.ltimestrs.append(datetime.utcfromtimestamp(self.ltimes[-1]).strftime('%Y-%m-%dT%H:%M:%S'))
                self.lpaths.append('raw/'+FILENAME)
                STAT=os.stat(FULLPATH)
                self.ldevs.append(STAT.st_dev)
                self.linos.append(STAT.st_ino)
                if os.path.islink(FULLPATH):
                    FILEPATH=os.readlink(FULLPATH)
                    self.lfilepaths.append(FILEPATH.split('storage/')[-1])
                else:
# Slow findpath replaced once we started writing down FILENUM (st_dev, st_iso combos) 
#                    FILEPATH=findpath(FULLPATH,self.storage)
                    self.lfilepaths.append('NONE')
                self.lnites.append(self.nite)
                self.nlinks += 1
            else:
                print(FULLPATH+' does not exist.')
        print(str(self.nlinks)+" found.")

    def count_links_search(self,DIRLIST=[]):
        self.ltimes=[]
        self.ltimestrs=[]
        self.lpaths=[]
        self.ldevs=[]
        self.linos=[]
        self.lfilepaths=[]
        self.lnites=[]
        self.nlinks=0
        REPODIRS =  ['raw/']
        if DIRLIST == []:
           DIRLIST = [REPODIR+self.nite for REPODIR in REPODIRS]
        for DIR in trimslash(DIRLIST):
            NITEDIR=self.repo_dir+DIR
            if os.path.exists(NITEDIR):
                for FILE in sorted(rec_listdir(NITEDIR)):
                    FULLPATH=NITEDIR+'/'+FILE
                    self.ltimes.append(os.lstat(FULLPATH).st_atime)
                    STAT=os.stat(FULLPATH)
                    self.ldevs.append(STAT.st_dev)
                    self.linos.append(STAT.st_ino)
                    self.ltimestrs.append(datetime.utcfromtimestamp(self.ltimes[-1]).strftime('%Y-%m-%dT%H:%M:%S'))
                    self.lpaths.append(DIR+'/'+FILE)
                    if os.path.islink(FULLPATH):
                        FILEPATH=os.readlink(FULLPATH)
                    else:
                        FILEPATH=findpath(FULLPATH,self.storage)
                    self.lfilepaths.append(FILEPATH.split('storage/')[-1])
                    if dirisnite(DIR.split('/')[-1]):
                        self.lnites.append(self.nite)
                    else:
                        self.lnites.append(timetonite(os.path.getmtime(FILEPATH)))
                    self.nlinks += 1
            else:
                print("Warning: "+NITEDIR+" does not exist.")


    def scan_log(self):
        if self.ingest_log is None:
           print("No ingest log. Not checking for ingestion errors.")
           return None
        if not os.path.exists(self.ingest_log): 
           print("No ingest log. Not checking for ingestion errors.")
           return None

    def update_db_files(self):
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        for num in range(len(self.filenames)):
            c.execute("INSERT OR REPLACE INTO TRANSFER_LIST (Filenum, Nite_Trans, Last_Update, Transfer_Path, Creation_Time, File_Size) VALUES("+str(self.tinos[num]*10000+self.tdevs[num])+", '"+self.tnites[num]+"', '"+self.nowstr+"', '"+self.filenames[num]+"', '"+self.ttimestrs[num]+"', "+str(self.filesizes[num])+")")
        conn.commit()
        conn.close()
        if len(self.filenames) > 0:
             print("Inserted or replaced "+str(len(self.filenames))+" into TRANSFER_LIST.")

    def update_db_gen3(self):
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        for num in range(len(self.ids)):
            query="INSERT OR REPLACE INTO FILE_LIST_GEN3 (Filenum, Nite, Last_Update, Transfer_Path, Status, Creation_Time, Transfer_Time, Ingest_Time, File_Size, Err_Message) VALUES("+str(self.ids[num])+", '"+self.nites[num]+"', '"+self.nowstr+"', '"+self.paths[num]+"', '"+self.statuss[num]+"', '"+self.ctimes[num]+"', '"+self.ttimes[num]+"', '"+self.itimes[num]+"', "+str(self.filesizes[num])+", '"+self.err_messages[num]+"')"
            c.execute(query)
        conn.commit()
        conn.close()
        if len(self.ids) > 0:
             print("Inserted or replaced "+str(len(self.ids))+" into FILE_LIST_GEN3.")

    def update_db_links(self):
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        for num in range(len(self.lpaths)):
            c.execute("INSERT OR REPLACE INTO INGEST_LIST (Filenum, Ingest_Path, Nite_Obs, Last_Update, Ingest_Time) VALUES("+str(self.linos[num]*10000+self.ldevs[num])+", '"+self.lpaths[num]+"', '"+self.lnites[num]+"', '"+self.nowstr+"', '"+self.ltimestrs[num]+"')") 
        conn.commit()
        conn.close()
        if len(self.lpaths) > 0:
             print("Inserted or replaced "+str(len(self.lpaths))+" into INGEST_LIST.")
    
    def get_night_data_gen3(self):
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        sizemin='10000'
        c.execute('select count(Transfer_Path), max(Creation_Time), sum(Status == "SUCCESS"), max(Transfer_Time), max(Ingest_Time), sum(substr(Transfer_Path,-5) != ".fits"), sum((substr(Transfer_Path,-5) == ".fits")*(File_Size < '+sizemin+')), sum((substr(Transfer_Path,-5) == ".fits")*(File_Size > '+sizemin+')*(Status != "SUCCESS")) from FILE_LIST_GEN3 t where Nite = "'+self.nite+'" group by Nite')
        rows=c.fetchall()
        if len(rows) > 0:
            [self.ntransfer, self.maxctime, self.ningest, self.maxttime, self.maxitime, self.nnotfits, self.nsmall, self.nerror]=rows[0]
        else:
            [self.ntransfer, self.maxctime, self.ningest, self.maxttime, self.maxitime, self.nnotfits, self.nsmall, self.nerror]=[0, '0000-00-00T00:00:00', 0, '0000-00-00T00:00:00','0000-00-00T00:00:00', 0, 0, 0 ]
        c.execute("INSERT OR REPLACE INTO FILE_COUNT_GEN3 (Nite_Obs, Last_Update, N_Files, Last_Creation, N_Ingest, N_Small, N_Not_Fits, N_Error, Last_Transfer, Last_Ingest) VALUES('"+self.nite+"', '"+self.nowstr+"', "+str(self.ntransfer)+", '"+self.maxctime+"', "+str(self.ningest)+", "+str(self.nsmall)+", "+str(self.nnotfits)+", "+str(self.nerror)+", '"+self.maxttime+"', '"+self.maxitime+"')")
        conn.commit()
        conn.close()

    def get_night_data(self):
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        c.execute('select count(Ingest_Path), max(Creation_Time), max(Ingest_Time) from INGEST_LIST i, TRANSFER_LIST t where i.FILENUM = t.FILENUM and Nite_Obs = "'+self.nite+'" group by Nite_Obs')
        rows=c.fetchall()
        if len(rows) > 0:
            [self.ningest,self.maxttime,self.maxitime,]=rows[0]
        else:
            [self.ningest,self.maxttime,self.maxitime]=[0,'0000-00-00T00:00:00','0000-00-00T00:00:00' ]
        sizemin='10000'
        c.execute('select count(FILENUM), max(Creation_Time), sum(File_Size < '+sizemin+'), sum(substr(Transfer_Path,-5) != ".fits"),  sum((substr(Transfer_Path,-5) == ".fits")*(File_Size > '+sizemin+')) from TRANSFER_LIST where FILENUM not in (select FILENUM from INGEST_LIST) and Nite_Trans = "'+self.nite+'" group by Nite_Trans')
        rows=c.fetchall()
        if len(rows) > 0:
            [ntransfer_unmatched, maxttime, self.nsmall,self.nnotfits,self.nerror] = rows[0]
            self.ntransfer=self.ningest+ntransfer_unmatched
            self.maxttime=max(self.maxttime,maxttime)
        else:
            self.ntransfer=self.ningest
            [self.nsmall,self.nnotfits,self.nerror]=[0,0,0]
        c.execute("INSERT OR REPLACE INTO FILE_COUNT (Nite_Obs, Last_Update, N_Files, Last_Creation, N_Ingest, N_Small, N_Not_Fits, N_Error, Last_Ingest) VALUES('"+self.nite+"', '"+self.nowstr+"', "+str(self.ntransfer)+", '"+self.maxttime+"', "+str(self.ningest)+", "+str(self.nsmall)+", "+str(self.nnotfits)+", "+str(self.nerror)+", '"+self.maxitime+"')")
        conn.commit()
        conn.close()

    def update_nite_html_gen3(self):
        outhtml=open(self.output_dir+'/'+self.nite+'_gen3.html','w')
        outhtml.write('<html>\n<head>\n<style>\n')
        outhtml.write('table, th, td {\nborder-collapse: collapse;\nfont-size: 20pt;\n }\n')
        outhtml.write('p {font-size: 20pt;\n }\n')
        outhtml.write('</style>\n')
        outhtml.write('</head>\n<body>\n')
        outhtml.write('<h1>'+self.name+' Transfer and Ingestion for Nite of '+self.nite+'</h1>') 
        outhtml.write('<p>Last updated: '+self.nowstr+'</p>\n')
        outhtml.write('<h2>Description</h2>\n')
        outhtml.write('<p>"File Name" is the name of the file transferred to NCSA.<br>\n')
        outhtml.write('"Status" is the result of the most recent ingestion attempt.<br>\n')
        outhtml.write('"File Size" is measured in bytes.<br>\n')
        outhtml.write('"Transfer Path" is the path of the transferred file at NCSA with root '+self.storage+'.<br>\n')
        outhtml.write('"Creation Time" is the UTC the file was created (based on Unix ctime).<br>\n')
        outhtml.write('"Transfer Time" is the UTC the file was created.<br>\n')
        outhtml.write('"Ingest Time" is the UTC of the file ingestion at NCSA.<br>\n')
        outhtml.write('"Delta Time 1" is the time (in seconds) between Creation and Transfer to NCSA (approximate transfer time).<br>\n')
        outhtml.write('"Delta Time 2" is the time (in seconds) between Transfer and Ingestion to NCSA (approximate ingest time).<br>\n')
        outhtml.write('"Err Message" is the Error Message from a failed ingestion attempt (if the most recent attempt failed).<br><br>\n')
        outhtml.write(db_to_html(self.db, ['select Transfer_Path, Status, File_Size, Creation_Time, Transfer_Time, Ingest_Time,  printf("%.1f",(julianday(Transfer_Time)-julianday(Creation_Time))*24*3600.) as Delta_Time_1,  printf("%.1f",(julianday(Ingest_Time)-julianday(Transfer_Time))*24*3600.) as Delta_Time_2, Err_Message from FILE_LIST_GEN3 where Nite = "'+self.nite+'" ORDER BY Creation_Time']))
        outhtml.write('</body>\n</html>')
        outhtml.close()

    def update_nite_html(self):
        outhtml=open(self.output_dir+'/'+self.nite+'.html','w')
        outhtml.write('<html>\n<head>\n<style>\n')
        outhtml.write('table, th, td {\nborder-collapse: collapse;\nfont-size: 20pt;\n }\n')
        outhtml.write('p {font-size: 20pt;\n }\n')
        outhtml.write('</style>\n')
        outhtml.write('</head>\n<body>\n')
        outhtml.write('<h1>'+self.name+' Transfer and Ingestion for Nite of '+self.nite+'</h1>') 
        outhtml.write('<p>Last updated: '+self.nowstr+'</p>\n')
        outhtml.write('<h2>Description</h2>\n')
        outhtml.write('<p>"File Name" is the name of the file transferred to NCSA.<br>\n')
        outhtml.write('"File Size" is measured in bytes.<br>\n')
        outhtml.write('"Transfer Path" is the path of the transferred file at NCSA with root '+self.storage+'.<br>\n')
        outhtml.write('"Creation Time" is the UTC the file was created.<br>\n')
        outhtml.write('"Ingest Path" is the path of the transferred file at NCSA with root '+self.repo_dir+'.<br>\n')
        outhtml.write('"Ingest Time" is the UTC of the file ingestion at NCSA.<br>\n')
        outhtml.write('"Delta Time" is the time (in seconds) between Creation and ingestion at NCSA (approximate transfer time).<br><br>\n')
        outhtml.write(db_to_html(self.db, ['select Transfer_Path, File_Size, Creation_Time, "None" as Ingest_Path, "None" as Ingest_Time, 0.0 as Delta_Time from TRANSFER_LIST where FILENUM not in (select FILENUM from Ingest_list) and Nite_Trans = "'+self.nite+'" ORDER BY Creation_Time', 'select Transfer_Path, File_Size, Creation_Time, Ingest_Path, Ingest_Time, printf("%.1f",(julianday(Ingest_Time)-julianday(Creation_Time))*24*3600.) as Delta_Time from Ingest_List i, Transfer_List t where i.FILENUM = t.FILENUM and Nite_Obs = "'+self.nite+'" ORDER BY Creation_time']))
        outhtml.write('</body>\n</html>')
        outhtml.close()

    def update_main_html_gen3(self):
        outhtml=open(self.output_dir+'/index_gen3.html','w')
        outhtml.write('<html>\n<head>\n<style>\n')
        outhtml.write('table, th, td {\nborder-collapse: collapse;\nfont-size: 20pt;\n }\n')
        outhtml.write('p {font-size: 20pt;\n }\n')
        outhtml.write('</style>\n')
        outhtml.write('</head>\n<body>\n')
        outhtml.write('<h1>'+self.name+' Transfer and Ingestion</h1>')
        outhtml.write('<p>Last updated: '+self.nowstr+'</p>\n')
        outhtml.write('<h2>Description</h2>\n')
        outhtml.write('<p>"Nite Obs" is the observing night (tied to UTC-12).<br>\n')
        outhtml.write('"Last Update" is the most recent UTC time that Nite Obs was checked.<br>\n')
        outhtml.write('"N Files" is the number of files received at NCSA.<br>\n')
        outhtml.write('"Last Creation" is the UTC of the most recent file creation.<br>\n')
        outhtml.write('"Last Transfer" is the UTC of the most recent file creation.<br>\n')
        outhtml.write('"N Ingest" is the number of files successfully ingested at NCSA.<br>\n')
        outhtml.write('"N Not Fits" is the number of uningested files that don\'t end in ".fits".<br>\n')
        outhtml.write('"N Small" is the number of ".fits" files smaller than 10K.<br>\n')
        outhtml.write('"N Error" is the number of uningested ".fits" files that are greater than 10K.<br>\n')
        outhtml.write('"Last Ingest" is the UTC of the most recently file ingest.<br><br>\n')
        outhtml.write(db_to_html(self.db, 'select * from FILE_COUNT_GEN3 ORDER by Nite_Obs DESC',linkify=True,modifier='_gen3'))
        outhtml.write('</body>\n</html>')
        outhtml.close()

    def update_main_html(self):
        outhtml=open(self.output_dir+'/index.html','w')
        outhtml.write('<html>\n<head>\n<style>\n')
        outhtml.write('table, th, td {\nborder-collapse: collapse;\nfont-size: 20pt;\n }\n')
        outhtml.write('p {font-size: 20pt;\n }\n')
        outhtml.write('</style>\n')
        outhtml.write('</head>\n<body>\n')
        outhtml.write('<h1>'+self.name+' Transfer and Ingestion</h1>') 
        outhtml.write('<p>Last updated: '+self.nowstr+'</p>\n')
        outhtml.write('<h2>Description</h2>\n')
        outhtml.write('<p>"Nite Obs" is the observing night (tied to UTC-12).<br>\n')
        outhtml.write('"Last Update" is the most recent UTC time that Nite Obs was checked.<br>\n')
        outhtml.write('"N Files" is the number of files received at NCSA.<br>\n')
        outhtml.write('"Last Creation" is the UTC of the most recent file creation.<br>\n')
        outhtml.write('"N Ingest" is the number of files successfully ingested at NCSA.<br>\n')
        outhtml.write('"N Small" is the number of uningested files smaller than 10K.<br>\n')
        outhtml.write('"N Not Fits" is the number of uningested files that don\'t end in ".fits".<br>\n')
        outhtml.write('"N Error" is the number of uningested ".fits" files that are greater than 10K.<br>\n')
        outhtml.write('"Last Ingest" is the UTC of the most recently file ingest.<br><br>\n')
        outhtml.write(db_to_html(self.db, 'select * from FILE_COUNT ORDER by Nite_Obs DESC',linkify=True))
        outhtml.write('</body>\n</html>')
        outhtml.close()

def main():
    config = get_config()
    num_days=countdays(config['num_days'], config['first_day'],config['last_day'])
    gen=config['gen']
    if (gen != 2) and ( gen != 3):
        print("'gen' variable must be either 2 or 3. Exiting.")
        sys.exit(1)    
    db_lines=db_filler(config['input_dir'],config['output_dir'],config['ingest_log'],config['gen'],config['last_day'])

    if gen == 2:
        for num in range(num_days):
            db_lines.set_date(num)
            db_lines.count_new_files()
            db_lines.update_db_files()
            if config['search_links']:
                db_lines.count_links_search()
            else:
                db_lines.count_links()
            db_lines.update_db_links()
        for num in range(num_days):
            db_lines.set_date(num)
            db_lines.get_night_data()
            db_lines.update_nite_html() 
        db_lines.update_main_html()
    if gen == 3:
        db_lines.set_date(num_days)
        db_lines.count_files_gen3()    
        db_lines.update_db_gen3() 
        for num in range(num_days):
            db_lines.set_date(num)
            db_lines.get_night_data_gen3()
            db_lines.update_nite_html_gen3() 
        db_lines.update_main_html_gen3()
    os.remove(db_lines.lock)
main()


# TEST COMMENT