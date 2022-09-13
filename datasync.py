import boto3
import time, os, requests

# bootstrap boto3
bot = boto3.session.Session(profile_name='PLACEHOLDER', region_name='PLACEHOLDER')
ec2 = bot.resource('ec2')

## TASK LIST
task_arn_list = ['PLACEHOLDER']

def ec2_bootstrap(ec2):
    """
    
    Save ec2 instance details that have the tag ops:AZURE and start them if not started

    """
    instances = []
    for instance in ec2.instances.filter(Filters=[{'Name': 'tag:ops', 'Values': ['AZURE']}]):
        if instance.state['Name'] == 'running':
            print('Instance {} is already running'.format(instance.tags[0]["Value"]))
        else:
            print('Starting instance {}'.format(instance.tags[0]["Value"]))
            instance.start()
            print('Waiting for instance to start')
            instance.wait_until_running()
            print('Instance {} is running'.format({instance.tags[0]["Value"]}))
        instances.append(instance)
    return instances


def slack_warning(token,channel_id,message):
    """
    Send a warning message to slack channel
    """
    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
    payload = {
        "channel": channel_id,
        "text": message,
    }
    response = requests.post('https://slack.com/api/chat.postMessage', json=payload, headers=headers)
    return response.json()

try:
    ## Bootstrap EC2 (grab ec2 instances)   
    instances = ec2_bootstrap(ec2)
except Exception as e:
    print('Error on EC2 bootstrap: {}'.format(e))
    message = 'Warning - EC2 bootstrap failed with message {}, get more info on: {}'.format(e,os.environ['CI_JOB_URL'])
    slack_warning(os.environ['SLACK_TOKEN'],os.environ['SLACK_CHANNEL'],message)
    exit(1)

# datasync execution
datasync = bot.client('datasync', region_name='us-west-2')

# Check for agents to be online
agent_offline = True
while agent_offline:
    try: 
        agents = datasync.list_agents()
    except Exception as e:
        print('DataSync error: {}'.format(e))
        message = 'Warning - Datasync agent listing failed with message {}, get more info on: {}'.format(e,os.environ['CI_JOB_URL'])
        slack_warning(os.environ['SLACK_TOKEN'],os.environ['SLACK_CHANNEL'],message)
        exit(1)
    for agent in agents['Agents']:
        if agent['Status'] != 'ONLINE':
            print("Agent {} with status {}".format(agent['Name'], agent['Status']))
            time.sleep(1)
        elif agent['Status'] == 'ONLINE':
            print("Agent {} with status {}".format(agent['Name'], agent['Status']))
            agent_offline = False
            time.sleep(1)


try:
    for task_arn in task_arn_list:
        datasync.start_task_execution(TaskArn=task_arn)
except Exception as e:
    print('DataSync error: {}'.format(e))
    message = 'Warning - Datasync execution failed with message {}, get more info on: {}'.format(e,os.environ['CI_JOB_URL'])
    slack_warning(os.environ['SLACK_TOKEN'],os.environ['SLACK_CHANNEL'],message)
    exit(1)

## CHECKER
db_checker = True
efs_checker = True

while db_checker or efs_checker:
    # DB checker
    print('Checking EFS & DB tasks...')
    tasks = datasync.list_tasks(MaxResults=100)['Tasks']
    for task in tasks:
        if task['Name'] == 'EFS-AZURE' or task['Name'] == 'DB-AZURE':
            print('Task found: ' + task['Name'])
            if task['Status'] != 'RUNNING':
                print('Tasks {} finished current status: {}'.format(task['Name'], task['Status']))
                if task['Name'] == 'EFS-AZURE':
                    efs_checker = False
                    instance = [instance for instance in instances if instance.tags[0]['Value'] == 'EC2-AZURE-SYNC-EFS'][0]
                    ec2.stop_instances(InstanceIds=[instance.id])
                elif task['Name'] == 'DB-AZURE':
                    db_checker = False
                    instance = [instance for instance in instances if instance.tags[0]['Value'] == 'EC2-AZURE-SYNC-DB'][0]
                    ec2.stop_instances(InstanceIds=[instance.id])
                elif task['Status'] != 'SUCCESS':
                    print('Something went wrong on task {}'.format(task['Name']))
                    message = "Warning - A the datasync task '{}' has failed, please look up {} for more information".format(task['Name'], os.environ['CI_JOB_URL'])
                    slack_warning(os.environ['SLACK_TOKEN'],os.environ['SLACK_CHANNEL'],message)
                    exit(1)
            else:
                print('Tasks {} still running'.format(task['Name']))
    time.sleep(300)
