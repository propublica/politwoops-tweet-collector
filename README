= Install Beanstalkd

http://kr.github.com/beanstalkd/download.html

Requires installing the libevent-dev package on apt-based systems.

= Install Python dependencies

If you're using pip, `pip install -r requirements.txt` should be sufficient.

* beanstalkc
* anyjson
* pyyaml
* boto
* tweetstream
* requests
* MySQLdb
* argparse (included in python 2.7+)

= Edit config file

In the [tweets-client] section, add your Twitter account's username and password. It is this account that will be authenticated against to make the API requests.

In the [beanstalk] section, change the parameters "tweets_tube" and "screenshot_tube". The values don't matter much, they just need to be unique.

In the [database] section, update the "host", "port", "username", "password", and "database" sections with your own details, if the defaults are not appropriate.

In the [aws] section, add your access key, secret access key, bucket name, and any path prefix inside the bucket you want to use. This is for archiving images and screenshots of tweeted links.

= Running

Run tweets-client.py to start streaming items from Twitter into the beanstalk queue. Append the lib directory to the PYTHONPATH, either persistently or as part of the command:

PYTHONPATH=$PYTHONPATH:`pwd`/lib ./bin/tweets-client.py

Then run politwoops-worker.py to start pulling the tweets out of beanstalk and loading them into MySQL:

PYTHONPATH=$PYTHONPATH:`pwd`/lib ./bin/politwoops-worker.py --images

Finally, if you ran politwoops-worker.py with the images option turned on, run screenshot-worker.py to grab screenshots of webpages and mirror images linked in tweets.

PYTHONPATH=$PYTHONPATH:`pwd`/lib ./bin/screenshot-worker.py

These three scripts all accept a --loglevel option for determining the verbosity of thier logging.They also accept a --output option to determine the destination of their logs. Finally, they all accept a --restart option that causes them to restart themselves if they encounter an error that cannot be handled.
