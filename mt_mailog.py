#!/export/data/python/3.6.4/bin/python3

__author__ = "James Dillon"

import datetime,os,smtplib,time, pickle
from mt_rmdir import print_statement, remove_dirs, removal_list
from datetime import date
from mt_cryptoPik import MongoHash
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders

class MongoMailer():
    def __init__(self):
        dicto = MongoHash().decipherPickle()
        self.gmpass = dicto['ogm'] 
        self.msg = MIMEMultipart()
        self.recipient = 'nersc-operator@lbl.gov'
        self.cc = ['jdillon@lbl.gov','pdillon222@gmail.com']
        self.cc.append('wbhimji@lbl.gov')  
        self.bcc = []
        self.date_stamp = datetime.datetime.now().strftime("%m/%d/%y_%H:%M PST")

    def generate_message(self,failure=False):
        if failure: 
            self.msg['From'] = 'MongoThread Daemon(Failure)' 
            self.msg['Subject'] = "MongoDB02 Backup Failure: {}".format(self.date_stamp)
            msg_string = "Notification of failed backup attempt of MongoDB02.\n\
Please see attached files for log data and stdout/stderr info."
            return msg_string 
        elif not failure:
            self.msg['From'] = 'MongoThread Daemon'
            self.msg['Subject'] = "MongoDB02 Successful Backup: {}".format(self.date_stamp)
            msg_string = "Notification of successful backup of MongoDB02.\n\
Please see attached files for log data and stdout/stderr info."
            msg_string += print_statement(removal_list())
            remove_dirs(removal_list())
            return msg_string

    def msg_send(self,message_function,file_attach_list):
        self.msg['To'] = self.recipient
        self.msg['Cc'] = ",".join(self.cc)
        #self.msg['Bcc'] = self.bcc

        text = MIMEMultipart('alternative') 
        text.attach(MIMEText(message_function)) #takes in the returned message body from self.generate_message        

        for file in file_attach_list:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(open(file, 'rb').read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % file)
            self.msg.attach(part)
    
        self.msg.attach(text) #attach the main message body, generated by return of function
        mail = smtplib.SMTP("smtp.gmail.com", 587)
        mail.starttls()
        #mail.login("nersc-operator@lbl.gov", "{}".format(self.gmpass)) #swap out for pickle object
        mail.login("jdillon@lbl.gov", "{}".format(self.gmpass)) #would like to switch out for NERSC operator, but not clear on pword
        mail.sendmail("jdillon@lbl.gov",[self.recipient]+self.cc, self.msg.as_string())
        mail.quit()
        
        
if __name__=='__main__':
    failmailer = MongoMailer()
    successmailer = MongoMailer()
    
    #test send a failure message
    failmailer.msg_send(failmailer.generate_message(failure=True),["mtLogs/02_15_18.db_bak","mtTransferOut/02_15_18.tfout"])
    
    time.sleep(10)

    #test send a succesful message
    successmailer.msg_send(successmailer.generate_message(),["/export/data/python/3.6.4/lib/python3.6/MongoThread/mtLogs/02_15_18.db_bak","/export/data/python/3.6.4/lib/python3.6/MongoThread/mtTransferOut/02_15_18.tfout"])
