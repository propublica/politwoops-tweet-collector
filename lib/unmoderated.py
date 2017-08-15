#Script to check the number of unreviewed tweets and email the admins if there are some


import MySQLdb
import tweetsclient
import smtplib
from email.mime.text import MIMEText
import ConfigParser
import pytz
import datetime

smtpconfig = ConfigParser.ConfigParser()
smtpconfig.read('conf/tweets-client.ini')
smtp = smtplib.SMTP(smtpconfig.get('moderation-alerts', 'mail_host'), smtpconfig.get('moderation-alerts', 'mail_port'))
smtp.login(smtpconfig.get('moderation-alerts', 'mail_username'), smtpconfig.get('moderation-alerts', 'mail_password'))

recipient = smtpconfig.get('moderation-alerts', 'unmoderated_recipient')
sender = smtpconfig.get('moderation-alerts', 'sender')
max_tweets = smtpconfig.getint('moderation-alerts', 'max_tweets')

config = tweetsclient.Config().get()
conn = MySQLdb.connect(
            host=config.get('database', 'host'),
            port=int(config.get('database', 'port')),
            db=config.get('database', 'database'),
            user=config.get('database', 'username'),
            passwd=config.get('database', 'password'),
            charset="utf8mb4",
            use_unicode=True
        )
cur = conn.cursor()
cur.execute("""SELECT * FROM `deleted_tweets` WHERE reviewed=0 """)
tweets = cur.fetchall()
unmoderated = len(tweets)

if unmoderated > max_tweets:
    tz = pytz.timezone(unicode('US/Eastern'))
    dtnow = datetime.datetime.now(tz)

    msg = MIMEText('', 'plain')
    msg['Date'] = dtnow.strftime("%a, %d %b %Y %H:%M:%S %z")
    msg['Subject'] = 'Politwoops Administration Alert: %s Unmoderated Tweets!' % unmoderated
    msg['From'] = 'PolitwoopsAdmin@politwoops.sunlightfoundation.com'
    msg['To'] = recipient

    smtp.sendmail(sender, recipient, msg.as_string())

    print "there are %s unreviewed tweets" % unmoderated
