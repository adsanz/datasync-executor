# Datasync executed
This script is being used to trigger datasync executions on a scheduled pipeline. On the main points where errors could happen, I added a helper function that sends a message to a slack channel of your choice. 

If you need to set-up a slack channel read the docs: https://slack.com/help/articles/115005265703-Create-a-bot-for-your-workspace

To execute you will need:
- boto3
- requests

Fill in the profile (or change it to env vars), and the `task_arn_list` variable. For agent bootstraping, we only use 2 agents, so I scrap all of them, PRs are welcome although I eventually will add filtering. 

Execute as: python3 datasync.py

Tested on python3.8 & 3.9

