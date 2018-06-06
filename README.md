# MongoThread
<hr><hr>

### A multi-threaded schema, for remote backup of MongoDB database collections

## Modules:

* main: Orchestrator of the modular program.  Run via command line by `./main.py`
* mt_listgen: Aggregator of current MongoDB databases within a given directory
* mt_tarfunc: Multi-threaded worker logic, executing file transfers to a remote server
* mt_dbconnect: Queries the replica DB's oplog, ensuring writes to the master DB do not fall past a given sync range
* mt_mailog: SMTP mail server, for mailing logs pertaining to backup data, and error messages to admins
* mt_cryptopik: Allows for creation of hashed password files, utilizing shared secret between remote server and host
* mt_rmdir: File rotation, deletes DB backups on remote server past a given date threshold
* mt_trtest: Asserts initially intended backup parameters, match that of the completed backups on remote server

<hr><hr>
