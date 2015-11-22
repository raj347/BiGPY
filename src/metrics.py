import requests
import json
from copy import copy
from datetime import datetime
import subprocess

APP="/applications"
JOBS="/applications/{app-id}/jobs"
JOB_DETAIL="/applications/{app-id}/jobs/{job-id}"
STAGES="/applications/{app-id}/stages"
STAGE_DETAIL="/applications/{app-id}/stages/{stage-id}"
STAGE_ATTEMPT="/applications/{app-id}/stages/{stage-id}/{stage-attempt-id}"
TASK_SUMMARY="/applications/{app-id}/stages/{stage-id}/{stage-attempt-id}/taskSummary"
TASK_LIST="/applications/{app-id}/stages/{stage-id}/{stage-attempt-id}/taskList"
EXECUTORS="/applications/{app-id}/executors"
RDD="/applications/{app-id}/storage/rdd"
RDD_ID="/applications/{app-id}/storage/rdd/{rdd-id}"
LOGS="/applications/{app-id}/logs"
ATTEMPT_LOGS="/applications/{app-id}/{attempt-id}/logs"

def req(end_point, master, args=None):
    if(args):
        end_point=end_point.format(**args)
    r = requests.get(master + end_point)
    print master + end_point
    #print r.text
    return json.loads(r.text)


def get_spark_state(master):
    spark_state = {}
    spark_state["apps"] = {}
    for app in req(APP, master):
        spark_state["apps"][app["name"]] = app
        args={}
        args["app-id"]=app['id']
        # Job details
        spark_state["apps"][app["name"]]["jobs"] = {}
        for job in req(JOBS,master,args):
            job_info = {}
            job_info["info"]=job
            job_args = copy(args)
            job_args["job-id"] = job["jobId"]
            job_info["detail"] = req(JOB_DETAIL,master,job_args)
            spark_state["apps"][app["name"]]["jobs"][job["jobId"]] = job_info
        # Stage details
        spark_state["apps"][app["name"]]["stages"] = {}
        for stage in req(STAGES,master,args):
            stage_info = {}
            stage_info["info"]=stage
            stage_args = copy(args)
            stage_args["stage-id"]=stage["stageId"]

            stage_detail=req(STAGE_DETAIL,master,stage_args)
            stage_info["detail"]=stage_detail
            stage_info["attempt"] = {}

            for stage_detail in stage_detail:
                stage_attempt_args = copy(stage_args)
                stage_attempt_args["stage-attempt-id"]=stage_detail["attemptId"]
                stage_info["attempt"]["details"] = req(STAGE_ATTEMPT,master,stage_attempt_args)
                stage_info["attempt"]["task_summary"]=req(TASK_SUMMARY,master,stage_attempt_args)
                stage_info["attempt"]["task_list"]=req(TASK_LIST,master,stage_attempt_args)

            spark_state["apps"][app["name"]]["stages"][stage["stageId"]] = stage_info
        return spark_state


def dump_data(master, input_file):
    job_persist_info = {}

    job_persist_info["time"]=datetime.now().strftime("%c")

    try:
        job_persist_info["number_of_nodes"]=int(subprocess.check_output('grep "#SBATCH --nodes" sketch.slurm | cut -d"=" -f 2', shell=True).strip())
    except Exception as e:
        print e
        raise Exception("Name the slurm file as sketch.slurm and make sure that you are executing from the src folder")
    try:
        job_persist_info["sequence_count"]=int(subprocess.check_output('wc -l %s' % input_file, shell=True).strip().split()[0])
    except Exception as e:
        print e
        raise Exception("IP file could could not be extracted")


    job_persist_info["spark_state"]=get_spark_state(master)



    persist_json={}
    with open('run_info.json', 'r') as outfile:
        persist_json=json.load(outfile)
    with open('run_info.json', 'w') as outfile:
        persist_json[datetime.now().strftime("%c")] = job_persist_info
        json.dump(persist_json, outfile, sort_keys=True, indent=4, separators=(',', ': '))
