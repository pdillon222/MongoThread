#!/export/data/python/3.6.4/bin/python3

__author__="James Dillon"

#ls /export/data/mongo | sed 's/\(\..*\)/ /' | uniq | wc -l
#nohup ./main.py 2>&1 >> mtTransferOut/$(date +"%m_%d_%y").tfout &

from MongoThread import mt_tarfunc
from mt_tarfunc import *
from mt_dbconnect import *
from mt_mailog import *
from mt_mailog import MongoMailer
from mt_dbconnect import MongoConnect
from concurrent import futures
from functools import partial
import os, datetime, sys, multiprocessing
from time import sleep


class MongoThreads(MongoTar):
    def __init__(self,mongo_dir):
        self.freeze_replica = datetime.datetime.now()
        MongoTar.__init__(self,mongo_dir,self.freeze_replica)
        self.mongo_dir = mongo_dir
        self.master_dict = self.dict_gen()
        self.file_string = "{}.db_bak".format(datetime.datetime.now().strftime("%m_%d_%y"))
        self.stdout_file = "/export/data/python/3.6.4/lib/python3.6/MongoThread/mtTransferOut/{}.tfout".format(datetime.datetime.now().strftime("%m_%d_%y"))
        self.tarobject = None
        #self.tarobject = MongoTar(self.mongo_dir,datetime.datetime.now())
        self.logFile = "/export/data/python/3.6.4/lib/python3.6/MongoThread/mtLogs/{}".format(self.file_string)
        self.file_attachments = [self.logFile,self.stdout_file]
        self.failmailer = MongoMailer()
        self.successmailer = MongoMailer()

    def send_fail(self):
        '''send e-mail message w/ attachments upon failed mongodb02 backup'''
        self.failmailer.msg_send(self.failmailer.generate_message(failure=True),self.file_attachments) 

    def send_success(self):
        '''send e-mail message w/ attachments upon successful mongodb02 backup'''
        self.successmailer.msg_send(self.successmailer.generate_message(),self.file_attachments)

    def sleep_test(self,minutes=60):
        '''upon unsuccesful run of the backup, sleep_test will initiate
           funcion will awaken periodically (dictated by minutes argument) to re-attempt backup
        '''
        while MongoConnect().oplog_query(MongoConnect().client,self.freeze_replica,printing=False):
            with open(self.logFile,"a") as log:
                print("{} -- *Not safe to lock replica, sleeping {} minutes.".format(datetime.datetime.now().strftime("[%H:%M:%S]"),minutes),file=log)
            sleep(minutes*60)
        with open(self.logFile,"a") as log:
                print("{} -- *Oplog header within safe date range, attempting backup.".format(datetime.datetime.now().strftime("[%H:%M:%S]"),minutes),file=log)
        self.initialize(MongoTar(self.mongo_dir,datetime.datetime.now()).list_chomp,self.dict_gen()[2],create_backup=True,create_dirs=True)    
        
    def initial_dirs(self): 
        '''I have inherited command_map from mt_tarfunc 
           and directory_keys attribute from mt_listgen
        '''
        for directory in self.directory_keys():
            self.cmd_map["directory"] = directory
            print("Processing database {directory}: creating {hpss_backupdir}/{backupname}/{directory} on {hpsshost}".format(**self.cmd_map))
            os.system("{hsicmd} -h {hpsshost} -q 'mkdir {hpss_backupdir}/{backupname}/{directory}'>/dev/null 2>&1".format(**self.cmd_map))    
    
    def process_pool(self,function,iterable):
        with open(self.logFile,"a") as log:
            print("{} -- Backup beginning.".format(datetime.datetime.now().strftime("[%H:%M:%S]")),file=log)

        if MongoConnect().oplog_query(MongoConnect().client,self.freeze_replica,printing=False):
            print("Oplog header date is below acceptable safety range for replica lock. Sleeping.")
            self.send_fail() #send failure e-mail message
            self.sleep_test() #replace later with a dedicated sleeper function 

        ########################
        MongoConnect().db_lock()
        ########################
        
        #main file transfer process
        with futures.ProcessPoolExecutor(max_workers=12) as pool: #change the max_workers later
            pool.map(function,iterable,timeout=10,chunksize=len(os.listdir(self.mongo_dir))//12)
        
        if MongoConnect().oplog_query(MongoConnect().client,self.freeze_replica,printing=False):
            #*************************
            MongoConnect().db_unlock()
            #*************************
            with open(self.logFile,"a") as log:
                print("{} -- !!Backup terminated: replica synchronization must begin now.".format(datetime.datetime.now().strftime("[%H:%M:%S]")),file=log)
                if os.system("{hsicmd} -h {hpsshost} 'ls {hpss_backupdir}/{backupname}' > /dev/null 2>&1".format(**self.cmd_map)) == 0:

                ###################################for now: deleting directory if it exists#####################################

                    os.system("{hsicmd} -h {hpsshost} 'rm {hpss_backupdir}/{backupname}/*/*' > /dev/null 2>&1".format(**self.cmd_map)) #erase contents within subs
                    os.system("{hsicmd} -h {hpsshost} 'rmdir {hpss_backupdir}/{backupname}/*' > /dev/null 2>&1".format(**self.cmd_map)) #erase directories within
                    os.system("{hsicmd} -h {hpsshost} 'rmdir {hpss_backupdir}/{backupname}' > /dev/null 2>&1".format(**self.cmd_map)) #erase today directory
            
            self.send_fail()
            self.sleep_test() #replace later with a dedicated sleeper function                             
 
        #smaller metadata transfer process
        with futures.ProcessPoolExecutor(max_workers=len(self.dict_gen()[1])) as dir_pool:
            dir_pool.map(self.tarobject.dir_handling,self.dict_gen()[1],timeout=10)

        #*************************
        MongoConnect().db_unlock()
        #*************************
     
        with open(self.logFile,"a") as log:
            print("{} -- Backup completed.".format(datetime.datetime.now().strftime("[%H:%M:%S]")),file=log) 

        self.send_success() #send succesful backup process e-mail confirmation

    def initialize(self,transfer_function,file_iterable,create_backup=True,create_dirs=True):
        self.tarobject = MongoTar(self.mongo_dir,datetime.datetime.now())
        if create_backup:self.backup_dir()
        if create_dirs:self.initial_dirs()
        self.pickle_dirs()
        self.process_pool(transfer_function,file_iterable)

if __name__=="__main__":
    thread_dir = MongoThreads('/export/data/mongo')
    thread_dir.initialize(thread_dir.list_chomp,thread_dir.dict_gen()[2],create_backup=True,create_dirs=True)
