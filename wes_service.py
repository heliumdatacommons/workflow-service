import connexion
from connexion.resolver import Resolver
import connexion.utils as utils

import threading
import tempfile
import subprocess
import uuid
import os
import json
import urllib

class Workflow(object):
    def __init__(self, workflow_id):
        super(Workflow, self).__init__()
        self.workflow_id = workflow_id
        self.workdir = os.path.join(os.getcwd(), "workflows", self.workflow_id)

    def run(self, request):
        os.makedirs(self.workdir)
        outdir = os.path.join(self.workdir, "outdir")
        os.mkdir(outdir)

        with open(os.path.join(self.workdir, "request.json"), "w") as f:
            json.dump(request, f)

        with open(os.path.join(self.workdir, "cwl.input.json"), "w") as inputtemp:
            json.dump(request["workflow_params"], inputtemp)

        if request.get("workflow_descriptor"):
            with open(os.path.join(self.workdir, "workflow.cwl"), "w") as f:
                f.write(workflow_descriptor)
                workflow_url = urllib.pathname2url(os.path.join(self.workdir, "workflow.cwl"))
        else:
            workflow_url = request.get("workflow_url")

        output = open(os.path.join(self.workdir, "cwl.output.json"), "w")
        stderr = open(os.path.join(self.workdir, "stderr"), "w")

        proc = subprocess.Popen(["cwl-runner", workflow_url, inputtemp.name],
                                stdout=output,
                                stderr=stderr,
                                close_fds=True,
                                cwd=outdir)
        output.close()
        stderr.close()
        with open(os.path.join(self.workdir, "pid"), "w") as pid:
            pid.write(str(proc.pid))

        return self.getstatus()

    def getstate(self):
        state = "Running"
        exit_code = -1

        exc = os.path.join(self.workdir, "exit_code")
        if os.path.exists(exc):
            with open(exc) as f:
                exit_code = int(f.read())
        else:
            with open(os.path.join(self.workdir, "pid"), "r") as pid:
                pid = int(pid.read())
            (_pid, exit_status) = os.waitpid(pid, os.WNOHANG)
            if _pid != 0:
                exit_code = exit_status >> 8
                with open(exc, "w") as f:
                    f.write(str(exit_code))
                os.unlink(os.path.join(self.workdir, "pid"))

        if exit_code == 0:
            state = "Complete"
        elif exit_code != -1:
            state = "Error"

        return (state, exit_code)

    def getstatus(self):
        state, exit_code = self.getstate()

        return {
            "workflow_id": self.workflow_id,
            "state": state
        }


    def getlog(self):
        state, exit_code = self.getstate()

        with open(os.path.join(self.workdir, "request.json"), "r") as f:
            request = json.load(f)

        with open(os.path.join(self.workdir, "stderr"), "r") as f:
            stderr = f.read()

        if state == "Complete":
            with open(os.path.join(self.workdir, "cwl.output.json"), "r") as outputtemp:
                outputobj = json.load(outputtemp)

        return {
            "workflow_id": self.workflow_id,
            "request": request,
            "state": state,
            "workflow_log": {
                "cmd": [""],
                "startTime": "",
                "endTime": "",
                "stdout": "",
                "stderr": stderr,
                "exitCode": exit_code
            },
            "task_logs": [],
            "outputs": outputobj
        }

    def cancel(self):
        pass

def GetServiceInfo():
    return {
        "workflow_type_versions": {
            "CWL": ["v1.0"]
        },
        "supported_wes_versions": "0.1.0",
        "supported_filesystem_protocols": ["file"],
        "engine_versions": "cwl-runner",
        "system_state_counts": {},
        "key_values": {}
    }

def ListWorkflows(body):
    # body["page_size"]
    # body["page_token"]
    # body["key_value_search"]

    wf = []
    for l in os.listdir(os.path.join(os.getcwd(), "workflows")):
        if os.path.isdir(os.path.join(os.getcwd(), "workflows", l)):
            wf.append(Workflow(l))
    return {
        "workflows": [{"workflow_id": w.workflow_id, "state": w.getstate()} for w in wf],
        "next_page_token": ""
    }

def RunWorkflow(body):
    if body["workflow_type"] != "CWL" or body["workflow_type_version"] != "v1.0":
        return
    workflow_id = uuid.uuid4().hex
    job = Workflow(workflow_id)
    job.run(body)
    return {"workflow_id": workflow_id}

def GetWorkflowLog(workflow_id):
    job = Workflow(workflow_id)
    return job.getlog()

def CancelJob(workflow_id):
    job = Workflow(workflow_id)
    job.cancel()
    return {"workflow_id": workflow_id}

def GetWorkflowStatus(workflow_id):
    job = Workflow(workflow_id)
    return job.getstatus()

def main():
    app = connexion.App(__name__, specification_dir='swagger/')
    def rs(x):
        return utils.get_function_from_name("wes_service." + x)

    app.add_api('proto/workflow_execution.swagger.json', resolver=Resolver(rs))

    app.run(port=8080)

if __name__ == "__main__":
    main()