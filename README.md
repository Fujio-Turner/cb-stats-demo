# cb-stats-demo
Mine Couchbase Server Status to a Log


**CONFIGURATION**
open the config.json file and update your settings.

{
"hostname":"127.0.0.1",   (Put in you hostname or ip of the machine it getting the stats from)
"username":"readOnly",    (Put in the read-only username)
"password":"password",    (Put in the read-only password)
"path":"/your/log/path/to/cb-stats-demo/logs/",
"debug":false,
"port":"8091",
"secure":false
}

**RUNNING**
Insert into your cron tab 
* * * * * /usr/bin/python /path/to/cb-stats-demo/cb-stats-demo.py


**FAQ**
Q:How big will the files/log get?
A:It will roughly get about 900KB per minute per bucket per server or about 1.1GB per day per bucket per server

Q:Will the file size grow forever?
A: No, it is bound to a particular day. Example 2016-06-17_cbstats.txt will only have the stats for that day per server

Q:How do get rid of the logs that are XYZ days old?
A:Linux has a built in log rotation tool on the folder level. In the future I'll come up with a built-in on.
https://www.cyberciti.biz/faq/how-do-i-rotate-log-files/
