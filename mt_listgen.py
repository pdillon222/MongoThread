#!/export/data/python/3.6.4/bin/python3

__author__="James Dillon"

import os,collections,sys,pickle,datetime

class MongoLists():
    def __init__(self,mongo_dir):
        self.mongo_dir = mongo_dir

    def dict_gen(self):
        '''
        Function dict_gen will create a dictionary mapping of MongoDB database names, and all of the DB's corresponding files
        Dictionary keys will be names of individual databases
        Dictionary values will be a single list, containing all files associated with the database
        There will be a few non-db keys, containing smaller lists of files associated with the key-name
        ''' 
        dir_list = [directory for directory in os.listdir(self.mongo_dir) if os.path.isdir(os.path.join(self.mongo_dir,directory))]
        db_names = {files[:files.index('.')] if '.' in files else files for files in os.listdir(self.mongo_dir)} 
        db_dict = collections.OrderedDict()
        for key in db_names:
            db_dict[key] = []
        for file_name in os.listdir(self.mongo_dir):
             if file_name not in dir_list:
                 if "." in file_name:
                     if file_name[:file_name.index('.')] in db_names:
                         db_dict[file_name[:file_name.index('.')]].append(file_name)
                 else:
                     db_dict[file_name].append(file_name)
        
        #failsafe, to ensure that all elements in /export/data/mongo have been added to lists:
        try: 
            assert sum([len(val) for key,val in db_dict.items()]) + len(dir_list) == len(os.listdir(self.mongo_dir))
        except AssertionError:
            print("DB dictionary mapping created with errors, Aborting backup!!") ##logging/email
            sys.exit()
        master_list = []
        for key in db_dict:
            master_list = master_list + db_dict[key]
        #ensure that no directories made their way into the master file list:
        try:
            assert not any(files in master_list for files in dir_list)
        except AssertionError:
            print("Error: generated list contains directories.  Aborting transfer")
            sys.exit()

        return db_dict,dir_list,master_list
        
    def directory_keys(self):
        '''Create and return key-names of directories to be created on HPSS'''
        key_list = {files[:files.index('.')] if '.' in files else files for files in os.listdir(self.mongo_dir)}
        key_list = [files for files in key_list]  
        return key_list

    def pickle_dirs(self):
       '''create a pickle object, listing the DB directories and subsequent files contained within'''
       '''will be used later for testing, to ensure successful transfer of all necessary files'''
       with open("/export/data/python/3.6.4/lib/python3.6/MongoThread/mtPickles/{}.pik".format(datetime.datetime.now().strftime("%m_%d_%y")),"wb") as pik:
           pickle.dump(self.dict_gen()[0],pik)

if __name__ == "__main__":
    #For displaying directory structure to be created on HPSS:
    mongo_items = MongoLists('/export/data/mongo')

    for key,val in mongo_items.dict_gen()[0].items():
        print("\n",key,":",len(val))
        print(val)

    print("\n","**",len(mongo_items.dict_gen()[0]),"database directories to be created:", "\n")#dictionary mapping
    print(mongo_items.directory_keys(),"\n")#directory list
    print("Directories to be handled after file transfers:\n",mongo_items.dict_gen()[1],"\n")

    #print(mongo_items.dict_gen()[2])#master file list, excluding directories

    #create the pickle object of directories and file lists:
    #mongo_items.pickle_dirs()


