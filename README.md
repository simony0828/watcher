# watcher
What is watcher?
It's a YAML Configuration style for waiting tables completion before the next ETL step is started.  Current support is for Snowflake only.

# how it works
Using the YAML configuration like this format:
```
sleep_time: 15
max_retry: 1

snowflake_tables:
  - schema.table1
  - schema.table2
```
Detail for execution:
- **sleep_time:** Number of minutes to wait between tries (default is 30 mins and only allow to choose between 15-90 mins)
- **max_retry:** Maximum retries when upstreams are not ready to run (default is 3 tries and only allow for up to 5 and 0 means no retry)
- **snowflake_tables:** A list of tables to wait for but ETL has to have a step to insert table completion to a meta table called `upload_history`

# how to run
```
python3 run_watcher.py -f <file> [--dry_run] [--unit_test] [--variable k1=v1] [--variable k2=v2] ...
```

> **--file / -f:**	The YAML configuration file containing all the data quality checks
> 
> **--dry_run / -d:**	Print all SQLs without executing in the system
> 
> **--unit_test / -u:**	Enable watcher without waiting too long
> 
> **--variable / -v:**	A variable list for string substitution
