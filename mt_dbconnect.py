#!/export/data/python/3.6.4/bin/python3

__author__="James Dillon"

'''
rs0:SECONDARY> db.fsyncUnlock()
{ "info" : "unlock completed", "ok" : 1 }
rs0:SECONDARY> db.currentOp()

see how far behind the backup is from the primary:
rs.printSlaveReplicationInfo() #returns lag between the primary and replica
'''

import pymongo
from pymongo import MongoClient
from pymongo.cursor import CursorType
from mt_cryptoPik import MongoHash
import datetime, time, sys,subprocess, os, pickle

class MongoConnect():
    def __init__(self):
        dicto = MongoHash().decipherPickle()
        self.dbpass = dicto['Mongo'] 
        uri = "mongodb://mongoadmin:{}@mongodb01.nersc.gov/admin?authMechanism=SCRAM-SHA-1".format(self.dbpass)
        uri2 = "mongodb://mongoadmin:{}@mongodb02.nersc.gov/admin?authMechanism=SCRAM-SHA-1".format(self.dbpass)
        self.client = MongoClient(uri)
        self.replica = MongoClient(uri2) 

    def db_lock(self):
        '''locks the replica from writes, no args allowed as we never want to lock db01'''
        print("Write-locking replica-set Mongodb02")
        try:
            self.replica.fsync(lock=True)
            ("Print replica-set succesfully locked")
            '''
            for db in [self.replica.admin.command('replSetGetStatus')["members"][0],self.replica.admin.command('replSetGetStatus')["members"][1]]:
                for key,val in db.items():
                    print(key,":",val)
            '''
        except:
            print("Mongodb02 could not be locked, aborting backup")
            sys.exit()

    def db_unlock(self):
        '''unlocks the replica, will raise an exception if db is not locked'''
        try:
            self.replica.unlock()
            print("Mongodb02 succesfully unlocked")
        except:
            print("Unable to unlock the db, please exit program and investigate through the Mongo shell")

    def replica_syncstatus(self,time_threshold = 120, printing=True):
        '''returns the synchronization state of the replica oplog'''
        replica_state = self.replica.admin.command('replSetGetStatus')
        sync_diff = (replica_state['date']-replica_state['members'][1]['optimeDate']).seconds
        if printing:
            print("\nreplica synchronization info")
            print("Synchronization date: {}".format(replica_state['members'][1]['optimeDate']))
            print("Health state: {}".format(replica_state['members'][1]['health']))
            print("Current optime: {}".format(replica_state['date']))
            print("Synchronization difference (replica-optime - mainDB-optime) in seconds: {}".format(sync_diff))

        return sync_diff > time_threshold

    def oplog_query(self,database,replica_freeze,days_threshold=5,printing=False,testing=False,thread_test=False):
        '''Queries the either the main or replica oplog, dictated by database argument
           allows specification of date threshold: how many days elapsed between now and header date in the oplog (10 by default)
           will return a boolean True if the number of days elapsed from now to the date indicated at start of oplog is > threshold
           thus a return of True indicates that it is safe to commence with fsync/write-lock and subsequent backup attempt
        '''
        #get header date for mainDB
        main_oplog = self.client.local.oplog.rs
        first = main_oplog.find().sort('$natural',pymongo.ASCENDING).limit(-1).next()
        main_ts = first['ts'] #header date of main oplog
        main_timestamp = datetime.datetime.fromtimestamp(main_ts.time)

        if printing:
            print_db_name = lambda db: print("\n","Initial oplog date for {} = {}".format(db,datetime.datetime.fromtimestamp(main_ts.time).strftime("%Y-%m-%d")))
            print_db_name(database)
            print("Replica freeze point: {}".format(replica_freeze))
            print("Oplog header date > {} from time of replica freeze: ".format(days_threshold),(replica_freeze-main_timestamp).days > days_threshold)
            print("Oplog header date = {}".format(main_timestamp))
            print("testing freeze-point - oplog header: {}".format(replica_freeze-main_timestamp))
            print("Boolean test (replica_freeze - oplog_header < days_threshold): {}".format((replica_freeze-main_timestamp).days < days_threshold))

        if testing:
            if os.access('/export/data/python/3.6.4/lib/python3.6/MongoThread/touchfile',os.F_OK): 
                return True #for testing process shutdown and exit procedures      
        
        #######the logic below is our critical test, as to whether or not backup can initialize as well as continue to run#######################################
        if thread_test and not testing: #the oplog query in the threads, should only test whether or not the main oplog header date is < days threshold
            return (replica_freeze-main_timestamp).days < days_threshold \
            or os.access('/export/data/python/3.6.4/lib/python3.6/MongoThread/killfile',os.F_OK) #allow for a killswitch for quick/clean termination
        
        #everywhere else, the oplog query should make sure both oplog header dates match
        return (replica_freeze-main_timestamp).days < days_threshold and not self.replica_syncstatus(printing=True) \
        or os.access('/export/data/python/3.6.4/lib/python3.6/MongoThread/killfile',os.F_OK) #allow for a killswitch for quick/clean termination
 
        #######consider changing the date threshold as necessary.  Backup will currently not run if oplog header is within 10 days of today######################

    def return_db(self):
        '''exports connection MongoClient instances of Mongodb01 and Mongodb02 respectively'''
        return self.client, self.replica

if __name__=="__main__":
    #testing some example usage
    mongodb01,mongodb02 = MongoConnect().return_db()
    print(MongoConnect().oplog_query(mongodb02,datetime.datetime.now(),printing=True))
    print("\nInitial replica state:")
    print(MongoConnect().replica_syncstatus())

    print("locking db02")
    MongoConnect().db_lock()
    for times in range(4):
        for i in range(15):
            print(i)
            time.sleep(1)
        print(MongoConnect().replica_syncstatus())
 
    print("unlocking db02")
    MongoConnect().db_unlock()
    for times in range(3):
        for i in range(15):
            print(i)
            time.sleep(1)
        print(MongoConnect().replica_syncstatus())

