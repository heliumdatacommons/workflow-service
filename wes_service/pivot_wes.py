from __future__ import print_function
import json
import os
import subprocess
import time
import logging
import uuid
import shutil
import connexion
import six
import json
import datetime
from multiprocessing import Process
import flask
import requests
from wes_service.util import WESBackend

logging.basicConfig(level=logging.DEBUG)
DEBUG = True

"""
Checks a request's authorization header for a valid APIKey value
If invalid returns {'status_code': 401, 'msg': <msg>}
If valid returns {'status_code': 200, 'user': <user>, 'key': <key>}
If error returns {'status_code': 500, 'msg': <msg>}
"""
def check_user_key():
    valid = False
    ret_invalid = {'msg': 'The request is unauthorized.', 'status_code': 401}
    authorization = connexion.request.headers.get('Authorization', None)
    if not authorization:
        return ret_invalid
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
    auth_headers = {'Authorization': 'Basic {}'.format(auth_service_key)}
    auth_service_url = helium_conf['auth_service_url']
    auth_response = requests.get(auth_service_url + '/apikey/verify?key=' + user_key, headers=auth_headers)
    if auth_response.status_code == 200:
        valid = True
    content = json.loads(auth_response.content.decode('utf-8'))
    if valid:
        if 'user_name' not in content:
            return ('Failed to check authorization', 500)
        return {
            'status_code': 200,
            'user': content['user_name'],
            'key': user_key
        }
    else:
        return ret_invalid

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
        ssh_pubkey = helium_conf.get('ssh_pubkey', None)
        # TODO pick up user from authorization key
        env = {
            'CHRONOS_URL': 'http://@chronos:8080',
            'IRODS_HOST': host,
            'IRODS_PORT': '1247',
            'IRODS_HOME': '/{}/home/{}'.format(zone, user),
            'IRODS_USER_NAME': user,
            'IRODS_PASSWORD': password,
            'IRODS_ZONE_NAME': zone
        }
        if ssh_pubkey:
            env['SSH_PUBKEY'] = ssh_pubkey
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
                        workflow_status, app_status = self._update_run_status(run['run_id'])
                        listobj = {
                            'workflow_id': run['run_id'],
                            'state': workflow_status
                        }
                        if DEBUG:
                            listobj['appliance_url'] = self.pivot_endpoint + '/wes-workflow-' + listobj['workflow_id'] + '/ui'
                            listobj['appliance_status'] = app_status
                            listobj['workflow_log'] = base_url + '/' + run['run_id']
                            listobj['start_time'] = run['start_time']
                            listobj['request'] = {'workflow_descriptor': run['request']['workflow_descriptor']}
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

        run_id = str(uuid.uuid4())
        rundir = self.workdir + '/' + run_id
        while os.path.exists(rundir):
            run_id = str(uuid.uuid4()) # enforce unique among existing runs
            rundir = workdir + '/' + run_id

        if not os.path.exists(rundir):
            os.mkdir(rundir)

        env = self.get_env_vars(None)

        jobstore_path = '/toil-intermediate/jobstore-wes-' + run_id
        # do cleanup if prior jobstore exists
        if os.path.exists(jobstore_path):
            print('removing jobstore: {}'.format(jobstore_path))
            shutil.rmtree(jobstore_path)

        # get run command
        cmd = ['sudo', 'docker', 'run', '--privileged']
        for k, v in six.iteritems(env):
            cmd.append('-e')
            cmd.append('{}={}'.format(k, v))

        cmd.extend(['heliumdatacommons/datacommons-base:latest', '_toil_exec'])

        stdout_path = rundir + '/stdout.txt'
        stderr_path = rundir + '/stderr.txt'
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
        # add WORKFLOW_NAME and PIVOT_URL so launcher can clean itself up
        env['WORKFLOW_NAME'] = appliance_id
        env['PIVOT_URL'] = self.pivot_endpoint
        workflow_container['env'] = env

        command = \
              'cwltoil --no-match-user --clean onSuccess --cleanWorkDir onSuccess ' \
            + '--jobStore {jobstore_path} --linkImports --not-strict ' \
            + ' --batchSystem chronos --defaultCores 16 --defaultMemory 32G --defaultDisk 30G ' \
            + '--workDir /toil-intermediate --outdir /toil-intermediate/wes_output ' \
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
        if response.status_code < 400:
            logging.debug('Sucessfully created appliance with id: {}'.format(appliance_id))
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

    """
    Returns:
        - None: if run container is not found in stored json
        - str status: if status is found on workflow container in stored json
    """
    def _get_stored_run_status(self, run):
        if not run or 'X-appliance_json' not in run or 'containers' not in run['X-appliance_json']:
            return None
        launcher_container = [c for c in run['X-appliance_json']['containers'] if c['id'] == 'toil-launcher']

        if len(launcher_container) == 0:
            status = None
        else:
            launcher = launcher_container[0]
            if 'status' not in launcher:
                launcher['status'] = 'SYSTEM_ERROR'
            status = launcher['status']
        logging.debug('_get_stored_run_status, status: {}'.format(status))
        return status

    """
    Returns:
        - (None, None): if error occurs querying for appliance in pivot
        - ('P_NOEXIST', None): if appliance is gone and no status is queryable
        - ('P_EXIST', None): if appliance exists but no launcher container is in it
        - ('P_EXIST', str status): if appliance exists and launcher container has a status
    """
    def _get_pivot_run_status(self, run):
        if not run:
            return None

        url = self.pivot_endpoint + '/wes-workflow-' + run['run_id']
        resp = requests.get(url)
        if resp.status_code == 404:
            # gone, no status available in pivot
            logging.info('appliance not found')
            return ('P_NOEXIST', None)
        elif resp.status_code > 400:
            logging.error('error querying for appliance')
            return (None, None)
        else:
            data = json.loads(resp.content.decode('utf-8'))
            toil_launcher = [c for c in data['containers'] if c['id'] == 'toil-launcher']
            if len(toil_launcher) == 0:
                logging.info('toil launcher not found in appliance for run_id: {}'.format(run['run_id']))
                return ('P_EXIST', 'SYSTEM_ERROR') # container is not in the appliance
            else:
                launcher = toil_launcher[0]

        if launcher['state'] == 'running':
            status = 'RUNNING'
        elif launcher['state'] == 'success':
            status = 'COMPLETE'
        elif launcher['state'] in ['pending', 'staging']:
            status = 'INITIALIZING'
        elif launcher['state'] == 'submitted':
            status = 'QUEUED'
        else:
            status = 'SYSTEM_ERROR'
        logging.info('_get_pivot_run_status, status: {}'.format(status))
        return ('P_EXIST', status)

    """
    Returns:
        - (None, None): error
        - (P_EXIST, ...): appliance still exists
        - (P_NOEXIST, ...): appliance does not exist
        - (..., str status): status of the run
    """
    def _update_run_status(self, run_id):
        run_json = '/toil-intermediate/wes/' + run_id + '/run.json'
        logging.info('_update_run_status for run_id: {}'.format(run_id))
        with open(run_json) as fin:
            run = json.load(fin)

        stored_status = self._get_stored_run_status(run)
        if stored_status not in ['COMPLETE', 'CANCELED']:
            logging.info('querying pivot for status for run_id: {}'.format(run_id))
            (pivot_exist, pivot_status) = self._get_pivot_run_status(run)
        else:
            logging.info('skipping query to pivot for run_id: {}, run is marked {}'.format(run_id, stored_status))
            # don't do whole query to pivot if run has finished
            # we know that the appliance was deleted so set P_NOEXIST
            (pivot_exist, pivot_status) = ('P_NOEXIST', stored_status)

        need_to_update_file = False
        # got a status from pivot and it didn't match stored one
        if pivot_status and pivot_status != stored_status:
            logging.debug('updating status of run [{}] to [{}]'.format(run_id, pivot_status))
            run['status'] = pivot_status
            need_to_update_file = True
        # got stored status but none from pivot, because skipped query or appliance was gone
        elif pivot_exist == 'P_NOEXIST' and stored_status:
            if stored_status in ['RUNNING', 'QUEUED', 'INITIALIZING']:
                logging.info('run is marked as not finished, but appliance doesn\'t exist, updating to canceled')
                run['status'] = 'CANCELED'
                need_to_update_file = True
            else:
                run['status'] = stored_status

        if need_to_update_file:
            logging.info('updating run file for run_id: {}'.format(run_id))
            with open(run_json, 'w') as fout:
                json.dump(run, fout)

        if not pivot_exist:
            pivot_exist = 'P_NOEXIST'

        return (run['status'], pivot_exist)

    # DEPRECATED
    def _get_pivot_job_status(self, run_id, run_json_to_update=None):
        url = self.pivot_endpoint + '/wes-workflow-'+run_id
        print('_get_pivot_job_status, url: {}'.format(url))
        response = requests.get(url)
        appliance_status = 'P_NOEXIST' # P_NOEXIST, P_EXIST
        with open(run_json_to_update) as fin:
            run = json.load(fin)
            run_launcher = [o for o in run['X-appliance_json']['containers'] if o['id'] == 'toil-launcher']
            if len(run_launcher) == 0:
                raise RuntimeError('toil-launcher not found in run file: {}'.format(run_json_to_update))
            else:
                run_launcher = run_launcher[0]

        if response.status_code == 404:
            print('job not found, return {}, P_NOEXIST'.format(run_launcher['status']))
            return run_launcher['status'], 'P_NOEXIST'
            #status = 'CANCELED'
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

#        if run_json_to_update:
#            with open(run_json_to_update, 'r') as fin:
#                run = json.load(fin)
#                #print(json.dumps(run))
#                run_launcher = [o for o in run['X-appliance_json']['containers'] if o['id'] == 'toil-launcher'][0]
        if status != run['status']:
            run_launcher['status'] = status
            run['status'] = status
            if status == 'CANCELED':
                run['end_time'] = datetime.datetime.now().isoformat()
                run['exit_code'] = 1
            elif status == 'COMPLETE':
                run['end_time'] = datetime.datetime.now().isoformat()
                run['exit_code'] = 0

            # clean up appliance
            if status in ['CANCELED', 'COMPLETE']:
                requests.delete(self.pivot_endpoint + '/wes-workflow-' + run_id)

        with open(run_json_to_update, 'w') as fout:
            json.dump(run, fout)
        return (status, appliance_status)

    def GetRunLog(self, run_id):
        #TODO careful with this data size, could be large
        MAX_STDOUT_LEN = 500 # lines of stdout to return
        stdout_path = '/toil-intermediate/wes/' + run_id + '/stdout.txt'
        if os.path.exists(stdout_path):
            with open(stdout_path) as stdout:
                out_data = stdout.read()
                out_data = '\n'.join(out_data.rsplit('\n', MAX_STDOUT_LEN)[1:])
        else:
            out_data = ''

        run_json = '/toil-intermediate/wes/'+run_id+'/run.json'
        if not os.path.exists(run_json):
            return {'msg': 'workflow run not found', 'status_code': 404}, 404
        run_status, app_status = self._update_run_status(run_id)
        with open(run_json) as fin:
            run = json.load(fin)

        log = {
            'workflow_id': run_id,
            'state': run_status,
            'appliance_status': app_status,
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
        if DEBUG:
            log['appliance_url'] = self.pivot_endpoint + '/wes-workflow-' + run_id + '/ui', # TODO Remove
        return log

    def CancelRun(self, run_id):
        logging.debug('cancel run')
        appliance_name = 'wes-workflow-' + run_id
        response = requests.delete(self.pivot_endpoint + '/' + appliance_name)
        if response.status_code == 404 or response.status_code < 400:
            print('success')
            run_json = '/toil-intermediate/wes/' + run_id + '/run.json'
            print('CancelRun updating status of {} to CANCELED'.format(run_id))
            with open(run_json) as f:
                run = json.load(f)
                run['status'] = 'CANCELED'
            with open(run_json, 'w') as fout:
                json.dump(run, fout)
            #self._get_pivot_job_status(run_id, run_json_to_update=run_json)
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
        state, _ = self._update_run_status(run_id)
        #with open(run_json) as f:
        #run = json.load(f)
        return {
            'workflow_id': run_id,
            'state': state
        }


def create_backend(app, opts):
    return PivotBackend(opts)
