import vertica_python
import sys
import os
import datetime
import smtplib
import email

# Vertica connection
def createVeticaConnection():
    conn_info = {'host':'172.25.4.115', 'port':5433, 'user':'dbadmin','password':'dbadmin','database':'dwp1','read_timeout':600,'unicode_error':'strict','ssl':False,'connection_timeout':10}
    connection = vertica_python.connect(**conn_info)
    return connection

# Email Error message
def sendErrorEmail(msgText, fileName):
    s = smtplib.SMTP(host='smtp-mail.outlook.com', port=587)
    s.starttls()
    s.login(account, password)
    msg = MIMEMultipart()
    msg['From']=MY_ADDRESS
    msg['To']='rob.pursel@roivant.com'
    msg['Subject']="ERROR: Error in ETL Process {0}".format(fileName)
    msg.attach(MIMEText(msgText, 'plain')
    s.send_message(msg)
    del msg

# Email Success message
def sendSuccessEmail(msgText, fileName):
    s = smtplib.SMTP(host='smtp-mail.outlook.com', port=587)
    s.starttls()
    s.login(account, password)
    msg = MIMEMultipart()
    msg['From']=MY_ADDRESS
    msg['To']='rob.pursel@roivant.com'
    msg['Subject']="SUCCESS: ETL Process {0}".format(fileName)
    msg.attach(MIMEText(msgText, 'plain')
    s.send_message(msg)
    del msg

def fileProcessed(fileNum, logNum, numRecs, asOfDate):
    currDate = datetime.datetime.now()
    conn = getVerticaConnection()
    cur = conn.cursor()
    cur.execute("INSERT INTO files_processed (rec_num, file_num, log_num, num_records, processed_date) VALUES (files_processed_seq.nextval, {0}, {1}, {2}, {3}, {4})".format(fileNum, logNum, numRecs, currDate, asOfDate)
    conn.commit()
    conn.close()


