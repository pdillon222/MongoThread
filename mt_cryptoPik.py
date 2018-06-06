#!/export/data/python/3.6.4/bin/python3

__author__="James Dillon"

import sys, os, pickle, subprocess, random, socket
from copy import deepcopy

class MongoHash():
    def __init__(self):
        pass

    @classmethod
    def makeHashDicts(cls):
        '''Create a single level hash table, seeded by 
           unique directory locale and hostname''' 
        random.seed(os.getcwd()+socket.gethostname())       
        initAlpha = [chr(i) for i in range(33,127)]
        scramAlpha = deepcopy(initAlpha) 
        random.shuffle(scramAlpha)
        try:
            assert initAlpha != scramAlpha
        except AssertionError:
            exit(1)
        return [initAlpha,scramAlpha]
   
    def makePassHashes(self,pword,printing=False):
        pwordorig = deepcopy(pword)
        pword.split()
        pword = [ MongoHash.makeHashDicts()[0].index(char) for char in pword ]
        try:
            assert pwordorig ==  "".join([MongoHash.makeHashDicts()[0][i] for i in pword])
        except AssertionError:
            exit(1)
        hashed_pword = "".join([MongoHash.makeHashDicts()[1][i] for i in pword])        
        ################################sanity check##############################
        if printing:
            print("\nOriginal passsword: "+pwordorig)
            print("Indexed password converted through encoding: "\
                   +"".join([MongoHash.makeHashDicts()[0][i] for i in pword]))
            print("Hashed password: "+ hashed_pword+"\n") 
        ##########################################################################
        return hashed_pword

    def decryptPassHashes(self,hashedPass):
        hashedPass.split()
        unhashedPass = [ MongoHash.makeHashDicts()[1].index(char) for char in hashedPass ]
        unhashedPass = "".join([MongoHash.makeHashDicts()[0][i] for i in unhashedPass])  
        return unhashedPass

    @staticmethod
    def mkPickleHash(hashedMongo, hashedGmail, pikfile="/export/data/python/3.6.4/lib/python3.6/MongoThread/.pfile"):    
        '''note that the program depends upon the existence of '.pfile'
           containing hashes of the MongoDB02 and gmail passwords'''
        with open(pikfile,"wb") as pf:
            pickle.dump({"Mongo":hashedMongo, "ogm":hashedGmail},pf)
        subprocess.call(['chmod', '0700', pikfile])
    
    def decipherPickle(self, pikfile="/export/data/python/3.6.4/lib/python3.6/MongoThread/.pfile",printing=False):
        '''function decrypts dictionary values returned from pikfile'''
        with open(pikfile,"rb") as pf:
            crypt_dict = pickle.load(pf)
        ##########sanity check########
        if printing:
            print(crypt_dict['Mongo'])
            print(crypt_dict['ogm'])
        ##############################
        crypt_dict['Mongo'] = self.decryptPassHashes(crypt_dict['Mongo'])
        crypt_dict['ogm'] = self.decryptPassHashes(crypt_dict['ogm'])
        return crypt_dict    

if __name__ == "__main__":
    #print(MongoHash().makeHashDicts())
    mongo_pass = input("Please enter DB password: ")
    gmail_pass = input("Please enter mail server admin password: ")
    hashed_mongo = MongoHash().makePassHashes(mongo_pass,printing=True)
    hashed_gmail = MongoHash().makePassHashes(gmail_pass,printing=True)
    print("\n")
    unhashed_mongo = MongoHash().decryptPassHashes(hashed_mongo)
    unhashed_gmail = MongoHash().decryptPassHashes(hashed_gmail)
    #print(unhashed_mongo)
    #print(unhashed_gmail)
    MongoHash().mkPickleHash(hashed_mongo, hashed_gmail, pikfile=".pfile")
    MongoHash().decipherPickle(pikfile=".pfile",printing=True)
