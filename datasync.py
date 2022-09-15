import boto3
import time, os, requests

# bootstrap boto3
bot = boto3.session.Session(profile_name='', region_name='us-west-2')
ec2 = bot.resource('ec2')
datasync = bot.client('datasync')
## VARIABLES
# TASK LIST
task_arn_list = []
results = {
    'Task failed': [],
    'Tasks ok': [],
    'Started at': time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
}
# CHECKER
db_checker = True
efs_checker = True
# Check for agents to be online
agent_offline = True
agent_online_count = 0

## FUNCTIONS ##

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


def slack_message(token,channel_id,message):
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

def tasks_execution_checker(task,datasync):
    """
    Control task execution status
    """
    tasks_execution_list = datasync.list_task_executions(TaskArn=task.get('TaskArn'),MaxResults=1)['TaskExecutions']
    for task_execution in tasks_execution_list:
        if task_execution['Status'] == 'RUNNING':
            print('Task executionfrom {} is running'.format(task['Name']))
            return "RUNNING"
        elif task_execution['Status'] == 'SUCCESS':
            print('Task executionfrom {} has finished'.format(task['Name']))
            return "SUCCESS"
        elif task_execution['Status'] == 'ERROR':
            print('Task executionfrom {} has failed'.format(task['Name']))
            return "ERROR"


def agent_checker(datasync,agent_online_count,agent_offline):
    """
    Check if agents are online
    """
    while agent_offline:
        agents = datasync.list_agents()
        agent_len = len(agents['Agents'])
        for agent in agents['Agents']:
            if agent['Status'] != 'ONLINE':
                print("Agent {} with status {}".format(agent['Name'], agent['Status']))
                agent_online_count = agent_online_count - 1
            elif agent['Status'] == 'ONLINE':
                print("Agent {} with status {}".format(agent['Name'], agent['Status']))
                agent_online_count = agent_online_count + 1
        if agent_online_count == agent_len:
            agent_offline = False
            print("All agents are online")
        time.sleep(1)

try:
    ## Bootstrap EC2 (grab ec2 instances)   
    instances = ec2_bootstrap(ec2)
except Exception as e:
    print('Error on EC2 bootstrap: {}'.format(e))
    message = '[DATASYNC PIPELINE] Warning - EC2 bootstrap failed with message {}, get more info on: {}'.format(e,os.environ['CI_JOB_URL'])
    slack_message(os.environ['SLACK_TOKEN'],os.environ['SLACK_CHANNEL'],message)
    exit(1)

try:
    ## Check if agents are online
    agent_checker(datasync,agent_online_count,agent_offline)
except Exception as e:
    print('DataSync error: {}'.format(e))
    message = '[DATASYNC PIPELINE] Warning - Datasync agent listing failed with message {}, get more info on: {}'.format(e,os.environ['CI_JOB_URL'])
    slack_message(os.environ['SLACK_TOKEN'],os.environ['SLACK_CHANNEL'],message)
    exit(1)

try:
    ## Start tasks
    for task_arn in task_arn_list:
        datasync.start_task_execution(TaskArn=task_arn)
except Exception as e:
    print('DataSync error: {}'.format(e))
    message = '[DATASYNC PIPELINE] Warning - Datasync execution failed with message {}, get more info on: {}'.format(e,os.environ['CI_JOB_URL'])
    slack_message(os.environ['SLACK_TOKEN'],os.environ['SLACK_CHANNEL'],message)
    exit(1)


while db_checker or efs_checker:
    # DB checker
    print('Checking EFS & DB tasks...')
    tasks = datasync.list_tasks(MaxResults=100)['Tasks']
    for task in tasks:
        if task['Name'] == 'EFS-AZURE' or task['Name'] == 'DB-AZURE':
            print('Task found: ' + task['Name'])
            if task['Status'] == 'AVAILABLE':
                print('Tasks {} finished current status: {}'.format(task['Name'], task['Status']))
                # Check task execution status after it's finished, stop the instance and set the checker to false. If it fails, send a warning message to slack
                if task['Name'] == 'EFS-AZURE' and efs_checker == True:
                    task_status = tasks_execution_checker(task,datasync)
                    if task_status == 'SUCCESS':
                        print('{} task has finished with no errors on execution'.format(task['Name']))
                        results['Tasks ok'].append(task['Name'])
                    elif task_status == 'ERROR':
                        print('{} task has finished with errors on execution'.format(task['Name']))
                        results['Task failed'].append(task['Name'])
                    efs_checker = False
                    instance = [instance for instance in instances if instance.tags[0]['Value'] == 'EC2-AZURE-SYNC-EFS'][0]
                    instance.stop()
                elif task['Name'] == 'DB-AZURE' and db_checker == True:
                    if task_status == 'SUCCESS':
                        print('{} task has finished with no errors on execution'.format(task['Name']))
                    elif task_status == 'ERROR':
                        print('{} task has finished with errors on execution'.format(task['Name']))
                    instance = [instance for instance in instances if instance.tags[0]['Value'] == 'EC2-AZURE-SYNC-DB'][0]
                    instance.stop()                    
                    db_checker = False
            elif task['Status'] == 'RUNNING':
                print('Tasks {} still running'.format(task['Name']))
            elif task['Status'] == 'UNAVAILABLE':
                # Unavailable status is not meaningful since when we stop the agent after it's finished, it will be unavailable always. So we just ignore it
                pass
    time.sleep(300)

message = "Finished, execution results:\nTask failed: {}\nTask success: {}\nStarted at: {}\nFinished at: {}".format(results['Task failed'],results['Tasks ok'],results['Started at'],time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
slack_message(os.environ['SLACK_TOKEN'],os.environ['SLACK_CHANNEL'],message)
