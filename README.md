# datasync-executor
This script is being used to trigger datasync executions on a scheduled pipeline. 

To execute you will need:
- boto3
- request

Fill in the profile (or change it to env vars), and the `task_arn_list` variable. For agent bootstraping, we only use 2 agents, so I scrap all of them, PRs are welcome although I eventually will add filtering. 

Execute as: python3 datasync.py

Tested on python3.8 & 3.9

