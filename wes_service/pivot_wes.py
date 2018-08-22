from __future__ import print_function
import flask
import json
import os
import subprocess
import requests
import time
import logging
import uuid
import shutil
import connexion
import six
import json
import datetime
from multiprocessing import Process
from wes_service.util import WESBackend

logging.basicConfig(level=logging.INFO)
DEBUG = True

def require_user_key(func):
    def wrapper(*args, **kwargs):
        valid = False
        unauthorized = ('The request is unauthorized.', 401)
        authorization = connexion.request.headers.get('Authorization', None)
        if not authorization:
            return unauthorized
        terms = authorization.split()
        if len(terms) == 1:
            user_key = terms[0]
        elif len(terms) >= 2:
            if terms[0] == 'APIKey':
                user_key = terms[1]
            else:
                return ('Malformed authorization', 400)

        # got a user key so check it
        with open('helium.json') as f:
            helium_conf = json.load(f)
	auth_service_key = helium_conf['auth_service_key']
        auth_headers = {'Authorization': 'Basic ' + auth_service_key}
	auth_service_url = helium_conf['auth_service_url']
        auth_response = requests.get(auth_service_url + '/apikey/verify?key=' + user_key, headers=auth_headers)
        if auth_response.status_code == 200:
            valid = True

        if valid:
            return func(*args, **kwargs)
        else:
            return unauthorized
    return wrapper

class PivotBackend(WESBackend):
    def __init__(self, opts):
        super(PivotBackend, self).__init__(opts)
        with open('helium.json') as f:
            helium_conf = json.load(f)
        self.pivot_endpoint = helium_conf['pivot_endpoint']
        self.workdir = '/toil-intermediate/wes'
        if not os.path.exists(self.workdir) or not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)

    def get_env_vars(self, authorization):
        with open('helium.json') as f:
            helium_conf = json.load(f)
        user = helium_conf['irods_user']
        password = helium_conf['irods_password']
        zone = helium_conf['irods_zone']
        host = helium_conf['irods_host']
        ssh_pubkey = helium_conf['ssh_pubkey']
        # TODO pick up user from authorization key
        env = { 
            'CHRONOS_URL': 'http://@chronos:8080',
            'IRODS_HOST': host,
            'IRODS_PORT': '1247',
            'IRODS_HOME': '/{}/home/{}'.format(zone, user),
            'IRODS_USER_NAME': user,
            'IRODS_PASSWORD': password,
            'IRODS_ZONE_NAME': zone,
            'SSH_PUBKEY': ssh_pubkey
        }
        return env


    def GetServiceInfo(self):
        return {
            'workflow_type_versions': {
                'CWL': {'workflow_type_version': ['v1.0']},
                #'WDL': {'workflow_type_version': ['v1.0']},
                #'PY': {'workflow_type_version': ['2.7']}
            },
            'supported_wes_versions': '0.3.0',
            'supported_filesystem_protocols': ['file', 'http', 'https'],
            'engine_versions': ['3.16.0'],
            'system_state_counts': {},
            'key_values': {}
        }

    def ListRuns(self, page_size=None, page_token=None, state_search=None):
        runs = []
        base_url = connexion.request.base_url
        for dirent in os.listdir(self.workdir):
            #print('checking {}'.format(dirent))
            d1 = os.path.join(self.workdir, dirent)
            if os.path.isdir(d1):
                run_json = os.path.join(d1, 'run.json')
                if os.path.exists(run_json):
                    with open(run_json) as f:
                        run = json.load(f)
                        #if run['status'] not in ['COMPLETE', 'CANCELED']:
                            # don't query for workflows that are terminated
                        workflow_status, app_status = self._get_pivot_job_status(run['run_id'], run_json_to_update=run_json)

                        listobj = {
                            'workflow_id': run['run_id'],
                            'state': workflow_status
                        }
                        if DEBUG:
                            listobj['appliance_url'] = self.pivot_endpoint + '/wes-workflow-' + listobj['workflow_id'] + '/ui'
                            listobj['appliance_status'] = app_status
                            listobj['workflow_log'] = base_url + '/' + run['run_id']
                            listobj['start_time'] = run['start_time']
                        runs.append(listobj)
                else:
                    print('{} is not a workflow run'.format(dirent))
            else:
                print('{} is not a directory'.format(self.workdir + dirent))

        return {'workflows': runs}

    def RunWorkflow(self):
        print('REQUEST: {}'.format(vars(connexion.request)))
	print('BODY: {}'.format(connexion.request.json))
        body = connexion.request.json #json.loads(connexion.request.json)
        joborder = body['workflow_params']
        env = self.get_env_vars(None)
        run_id = str(uuid.uuid4())
        # do cleanup
        jobstore_path = '/toil-intermediate/jobstore-wes-'+run_id
        if os.path.exists(jobstore_path):
            print('removing jobstore: {}'.format(jobstore_path))
            shutil.rmtree(jobstore_path)
        

        # get run command
        cmd = ['sudo', 'docker', 'run', '--privileged']
        for k,v in six.iteritems(env):
            cmd.append('-e')
            cmd.append('{}={}'.format(k,v))

        cmd.extend(['heliumdatacommons/datacommons-base:latest', '_toil_exec'])
        #wf_location = 'https://raw.githubusercontent.com/DataBiosphere/topmed-workflows/d40a05772f5c832fe1bdce898aaa19698b43edbd/aligner/sbg-alignment-cwl/topmed-alignment.cwl'
        jobinput_location = '/renci/irods/home/wes_user/NWD176325-0005-recab.json'

        rundir = '{workdir}/{run_id}'.format(workdir=self.workdir, run_id=run_id)
        stdout_path = rundir + '/stdout.txt'
        stderr_path = rundir + '/stderr.txt'
        if not os.path.exists(rundir):
            os.mkdir(rundir)
        
        # create empty stdout file to avoid filenotfound later
        open(stdout_path, 'a').close()

        wf_location = body['workflow_url']

        # create job file
        env['JOBORDER'] = json.dumps(joborder)
        joborder_location = rundir + '/joborder.json'
        with open(joborder_location, 'w') as joborder_out:
            json.dump(joborder, joborder_out)

        # set up appliance json
        with open('pivot-template.json') as fin:
            appliance_json = json.load(fin)
        # fill out values
        appliance_id = 'wes-workflow-' + run_id
        appliance_json['id'] = appliance_id
        # get reference to container used to run workflow and temporarily remove from appliance
        workflow_container = [j for j in appliance_json['containers'] if j['id'] == 'toil-launcher'][0]
        appliance_json['containers'].remove(workflow_container)

        # add environment variables to container
        workflow_container['env'] = env

        command = \
              'cwltoil --no-match-user --clean onSuccess --cleanWorkDir onSuccess ' \
            + '--jobStore {jobstore_path} --linkImports --not-strict ' \
            + ' --batchSystem chronos --defaultCores 16 --defaultMemory 32G --defaultDisk 30G ' \
            + '--workDir /toil-intermediate --outdir /renci/irods/home/wes_user/toil_output ' \
            + '{workflow_location} {jobinput_location} 2>&1 | tee {stdout}'
        command = command.format(
                jobstore_path=jobstore_path,
                workflow_location=wf_location,
                jobinput_location=joborder_location,
                stdout=stdout_path)
        workflow_container['args'] = [
            '_toil_exec',
            command
        ]
        # add in the actual container used to run the workflow
        appliance_json['containers'].append(workflow_container)
        print(json.dumps(appliance_json, indent=4))
        response = requests.post(self.pivot_endpoint, data=json.dumps(appliance_json))
        if response.status_code <  400:
            print('SUCCESS')
        else:
            raise RuntimeError('failed to create appliance: {}:{}'.format(str(response), str(response.content)))
        # used to keep track of existing runs
        log_entry = {
            'run_id': run_id,
            'appliance_id': appliance_id,
            'X-appliance_json': appliance_json,
            'stdout': stdout_path,
            #'stderr': stderr_path,
            'command': command,
            'start_time': datetime.datetime.now().isoformat(),
            'end_time': '',
            'status': 'QUEUED',
            'exit_code': -1,
            'request': {
                'workflow_descriptor': body.get('workflow_descriptor'),
                'workflow_params': joborder,
                'workflow_type': body.get('workflow_type'),
                'workflow_type_version': body.get('workflow_type_version'),
                'workflow_url': body.get('workflow_url')
            }
        }
        log_location = rundir + '/run.json'
        with open(log_location, 'w') as run_out:
            json.dump(log_entry, run_out)

        return {'run_id': run_id}

    def _get_pivot_job_status(self, run_id, run_json_to_update=None):
        url = self.pivot_endpoint + '/wes-workflow-'+run_id
        response = requests.get(url)
        appliance_status = 'P_NOEXIST' # P_NOEXIST, P_EXIST
        if response.status_code == 404:
            status = 'CANCELED'
        elif response.status_code > 400:
            raise RuntimeError('error querying pivot: [{}]'.format(url))
        else:
            appliance_status = 'P_EXIST'
            data = json.loads(response.content.decode('utf-8'))
            launcher = [c for c in data['containers'] if c['id'] == 'toil-launcher']
            if len(launcher) == 0: # no launcher container was part of this appliance
                launcher = {'state':'COMPLETED'}
                # bad state, do cleanup
            else:
                launcher = launcher[0]

            if launcher['state'] == 'running':
                status = 'RUNNING'
            elif launcher['state'] == 'success':
                status = 'COMPLETE'
            elif launcher['state'] == 'pending':
                status = 'INITIALIZING'
            elif launcher['state'] == 'submitted':
                status = 'QUEUED'
            else:
                status = 'SYSTEM_ERROR'
                print('_get_pivot_job_status, status: {}'.format(status))
                print('_get_pivot_job_status: {}'.format(response.content.decode('utf-8')))

        if run_json_to_update:
            with open(run_json_to_update, 'r') as fin:
                run = json.load(fin)
                #print(json.dumps(run))
                run_launcher = [o for o in run['X-appliance_json']['containers'] if o['id'] == 'toil-launcher'][0]
                if status != run['status']:
                    run_launcher['status'] = status
                    if status == 'CANCELED':
                        run['end_time'] = datetime.datetime.now().isoformat()
                        run['exit_code'] = 1
                    elif status == 'COMPLETE':
                        run['end_time'] = datetime.datetime.now().isoformat()
                        run['exit_code'] = 0

            with open(run_json_to_update, 'w') as fout:
                json.dump(run, fout)
        return (status, appliance_status)

    def GetRunLog(self, run_id):
        #TODO careful with this data size, could be large
        stdout_path = '/toil-intermediate/wes/' + run_id + '/stdout.txt'
        if os.path.exists(stdout_path):
            with open(stdout_path) as stdout:
                out_data = stdout.read()
        else:
            out_data = ''

        run_json = '/toil-intermediate/wes/'+run_id+'/run.json'
        if not os.path.exists(run_json):
            return {'msg': 'workflow run not found', 'status_code': 404}, 404
        status, _ = self._get_pivot_job_status(run_id, run_json_to_update=run_json)
        with open(run_json) as fin:
            run = json.load(fin)

        return {
                'workflow_id': run_id,
                'state': status,
                'request': run['request'],
                'workflow_log': {
                    'name': 'stdout',
                    'start_time': run['start_time'],
                    'end_time': run['end_time'],
                    'exit_code': run['exit_code'],
                    'stdout': out_data
                }
                #outputs
                #task_logs
        }

    def CancelRun(self, run_id):
        appliance_name = 'wes-workflow-' + run_id
        response = requests.delete(self.pivot_endpoint + '/' + appliance_name)
        if response.status_code < 400:
            print('success')
            run_json = '/toil-intermediate/wes/'+run_id+'/run.json'
            #with open(run_json, r) as f:
            #    run = json.load(f)
            #    run['status'] = 'CANCELED'
            self._get_pivot_job_status(run_id, run_json_to_update=run_json)
            return {'workflow_id': run_id}
        else:
            return {
                'msg': 'failed to cancel workflow: {}: {}'.format(str(response), str(response.content)),
                'status_code': response.status_code
            }

    def GetRunStatus(self, run_id):
        run_json = '/toil-intermediate/wes/' + run_id + '/run.json'
        if not os.path.exists(run_json):
            return {
                'msg': 'The requested Workflow wasn\'t found',
                'status_code': 404
            }, 404
        state, _ = self._get_pivot_job_status(run_id, run_json_to_update=run_json)
        with open(run_json) as f:
            run = json.load(f)
        return {
            'workflow_id': run_id,
            'state': state
        }


def create_backend(app, opts):
    return PivotBackend(opts)
