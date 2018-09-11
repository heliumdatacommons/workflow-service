from __future__ import print_function
import fcntl
import re
import json
import random
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
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
        self.statedirs = ['/nfs-aws', '/nfs-gcp']
        self.workdir = '/wes'
        if not os.path.exists(self.workdir) or not os.path.isdir(self.workdir):
            raise RuntimeError('please create /wes and chown to the user running the wes service')
        if not os.path.exists(self.workdir + '/statemapping.json'):
            with open(self.workdir + '/statemapping.json', 'w') as mapout:
                json.dump({}, mapout)
        if not os.path.exists(self.workdir + '/schedulingstate.json'):
            with open(self.workdir + '/schedulingstate.json', 'w') as fout:
                json.dump({}, fout)

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
            #'CHRONOS_URL': 'http://@chronos:8080',
            'TOIL_WORKER_IMAGE': 'heliumdatacommons/datacommons-base:dev',
            'TOIL_NFS_WORKDIR_SERVER': '@nfsd:/data',
            'TOIL_NFS_WORKDIR_MOUNT': '/nfsd',
            'CHRONOS_URL': self._select_chronos_instance(),
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
        for statedir in self.statedirs:
            for dirent in os.listdir(statedir + '/wes/'):
                d1 = os.path.join(statedir + '/wes/', dirent)
                if os.path.isdir(d1):
                    run_json = os.path.join(d1, 'run.json')
                    if os.path.exists(run_json):
                        with open(run_json) as f:
                            run = json.load(f)
                            workflow_status, app_status = self._update_run_status(run['run_id'], run_json)
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
                        logger.warn('{} is not a workflow run'.format(dirent))
                else:
                    logger.warn('{} is not a directory'.format(d1))

        return {'workflows': runs}

    def _select_chronos_instance(self):
        with open('helium.json') as fin:
            conf = json.load(fin)
        scheduling_file = self.workdir + '/schedulingstate.json'
        fin = open(scheduling_file)
        fcntl.flock(fin, fcntl.LOCK_EX) # lock state file
        scheduling = json.load(fin)
        instances = conf['chronos_instances']
        instances.sort()
        
        if 'last_used' in scheduling:
            instances.append(scheduling['last_used'])
            instances = list(set(instances))
            instances.sort()
            ni = instances.index(scheduling['last_used']) + 1
            if ni > len(instances) - 1:
                ni = 0
            n = instances[ni]
            scheduling['last_used'] = n
        else:
            n = instances[0]
            scheduling['last_used'] = n
        logger.info('writing to schedlingstate.json: ' + json.dumps(scheduling))
 
        with open(scheduling_file, 'w') as fout:
            json.dump(scheduling, fout)
        # unlock state file
        fcntl.flock(fin, fcntl.LOCK_UN)
        fin.close()
        return n

        """
        min_jobs = {'url': None, 'count': None}
        for inst in conf['chronos_instances']:
            resp = requests.get(inst + '/v1/scheduler/jobs')
            if resp.status_code > 400:
                logger.error('failed to fetch chronos job list: ' + str(resp.status_code)
                    + '\n' + str(resp.content.decode('utf-8')))
                continue
            try:
                job_list = json.loads(resp.content.decode('utf-8'))
            except ValueError:
                logger.error('could not decode response content: '  + resp.content.decode('utf-8'))
                continue
            count = len(job_list)
            if min_jobs['count'] is None or count < min_jobs['count']:
                min_jobs['url'] = inst
                min_jobs['count'] = count
        if min_jobs['url'] is None:
            raise RuntimeError('no chronos servers responded to query')
        return min_jobs['url']
        """
        #inst = random.choice(conf['chronos_instances'])
        #return inst

    def _get_statedir(self, run_id):
        with open(self.workdir + '/statemapping.json') as fin:
            mapping = json.load(fin)
            if run_id in mapping:
                return mapping[run_id]
        # no stored mapping, attempt to find
        for statedir in self.statedirs:
            p = statedir + '/wes/' + run_id
            if os.path.exists(p):
                mapping[run_id] = statedir
            with open(self.workdir + '/statemapping.json', 'w') as fout:
                json.dump(mapping, fout)
            return statedir
        return None

    def RunWorkflow(self):
        with open('helium.json') as fin:
            config = json.load(fin)
        logger.debug('REQUEST: {}'.format(vars(connexion.request)))
        logger.debug('BODY: {}'.format(connexion.request.json))
        body = connexion.request.json #json.loads(connexion.request.json)
        joborder = body['workflow_params']

        env = self.get_env_vars(None)

        # set up appliance json
        with open('pivot-template-2.json') as fin:
            appliance_json = json.load(fin)
        # fill out values
        # get reference to container used to run workflow and temporarily remove from appliance
        workflow_container = [j for j in appliance_json['containers'] if j['id'] == 'toil-launcher'][0]
        #appliance_json['containers'].remove(workflow_container)

        # tags
        tags = {}
        if 'tags' in body:
            tags = body['tags']

        # add toil_cloud_constraint from tags, or default_toil_cloud config, or 'aws'
        if 'toil_cloud_constraint' in tags:
            logger.info('tags specified toil_cloud_constraint: ' + tags['toil_cloud_constraint'])
            env['TOIL_CLOUD_CONSTRAINT'] = tags['toil_cloud_constraint']
        else:
            default_toil_cloud = config.get('default_toil_cloud', 'aws')
            logger.info('setting default toil_cloud_constraint: ' + default_toil_cloud)
            env['TOIL_CLOUD_CONSTRAINT'] = default_toil_cloud

        # add appliance_cloud_constraint from tags, or default_appliance_cloud, or 'aws'
        if 'appliance_storage_cloud_constraint' not in tags:
            tags['appliance_storage_cloud_constraint'] = config.get('default_storage_appliance_cloud', 'aws')
            logger.info('setting default appliance_storage_constraint: ' + tags['appliance_storage_cloud_constraint'])
        if 'appliance_compute_cloud_constraint' not in tags:
            tags['appliance_compute_cloud_constraint'] = config.get('default_compute_appliance_cloud', 'aws')
            logger.info('settings default appliance_compute_constraint: ' + tags['appliance_compute_cloud_constraint'])
        for container in appliance_json['containers']:
            if 'nfsd' in container['id']:
                logger.info('Setting storage constraint: ' + tags['appliance_storage_cloud_constraint'])
                container['cloud'] = tags['appliance_storage_cloud_constraint']
            elif 'toil-launcher' in container['id']:
                logger.info('Setting compute constraint: ' + tags['appliance_compute_cloud_constraint'])
                container['cloud'] = tags['appliance_compute_cloud_constraint']
            else:
                container['cloud'] = tags['appliance_compute_cloud_constraint']

        # determine which NFS server to use for tracking this run
        if 'aws' in container['cloud']:
            state_directory = '/nfs-aws'
        elif 'gcp' in container['cloud']:
            state_directory = '/nfs-gcp'
        else:
            raise RuntimeError('could not determine state directory to use')
        logger.debug('using {} for run states'.format(state_directory))

        # create unique run_id and run directory
        run_id = str(uuid.uuid4())
        rundir = state_directory + '/wes/'  + run_id
        while os.path.exists(rundir):
            run_id = str(uuid.uuid4()) # enforce unique among existing runs
            rundir = state_directory + '/wes/' + run_id
        if not os.path.exists(rundir):
            os.mkdir(rundir)
        logger.info('this run is stored in {}'.format(rundir))

        # remember which nfs directory used for this run
        with open(self.workdir + '/statemapping.json', 'r') as mapin:
            mapping = json.load(mapin)
            mapping[run_id] = state_directory
        with open(self.workdir + '/statemapping.json', 'w') as mapout:
            json.dump(mapping, mapout)

        # Replace the run uuid in the appliance json
        appliance_id = 'wes-workflow-' + run_id
        appliance_json['id'] = appliance_id
        nfsd_container = [j for j in appliance_json['containers'] if j['id'] == 'nfsd'][0]
        nfsd_container['volumes'][0]['host_path'] = nfsd_container['volumes'][0]['host_path'].replace('wf-uuid', run_id)
        
        # replace the run uuid in the toil-launcher appliance json
        for v in workflow_container['volumes']:
            if 'wf-uuid' in v['host_path']:
                v['host_path'] = v['host_path'].replace('wf-uuid', run_id)

        # determine jobstore location
#        if not os.path.exists(state_directory + '/jobstores'):
#            os.mkdir(state_directory + '/jobstores')
#        jobstore_path = state_directory + '/jobstores/jobstore-wes-' + run_id
#        # do cleanup if prior jobstore exists
#        if os.path.exists(jobstore_path):
#            logger.info('removing jobstore: {}'.format(jobstore_path))
#            shutil.rmtree(jobstore_path)

        # get run command
        cmd = ['sudo', 'docker', 'run', '--privileged']
        for k, v in six.iteritems(env):
            cmd.append('-e')
            cmd.append('{}={}'.format(k, v))

        cmd.extend(['heliumdatacommons/datacommons-base:dev', '_toil_exec'])

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


        output_path = state_directory + '/output-' + run_id
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        command = \
              'cwltoil --no-match-user  ' \
            + '--jobStore {nfsd_mount}/jobstore --not-strict ' \
            + ' --batchSystem chronos --defaultCores 8 --defaultMemory 16G --defaultDisk 16G ' \
            + '--workDir /tmp --outdir {output_path} ' \
            + '--tmpdir-prefix=/tmp/tmpdir --tmp-outdir-prefix={nfsd_mount}/tmpdir_out ' \
            + '{workflow_location} {jobinput_location} 2>&1 | tee {stdout}'
        # all workers just have 1 nfs server mounted to /toil-intermediate
        def convert_local_nfs_path_to_worker(path):
            return re.sub(r'/nfs-[^/]+', '/toil-intermediate', path)
        command = command.format(
            output_path=convert_local_nfs_path_to_worker(output_path),
            #jobstore_path=convert_local_nfs_path_to_worker(jobstore_path),
            workflow_location=wf_location,
            jobinput_location=convert_local_nfs_path_to_worker(joborder_location),
            stdout=convert_local_nfs_path_to_worker(stdout_path),
            nfsd_mount=env.get('TOIL_NFS_WORKDIR_MOUNT', '/toil-intermediate'))
        workflow_container['args'] = [
            '_toil_exec',
            command
        ]

        # add WORKFLOW_NAME and PIVOT_URL so launcher can clean itself up
        env['WORKFLOW_NAME'] = appliance_id
        env['PIVOT_URL'] = self.pivot_endpoint

        workflow_container['env'] = env

        # add in the actual container used to run the workflow
        #appliance_json['containers'].append(workflow_container)
        logger.debug(json.dumps(appliance_json, indent=4))

        response = requests.post(self.pivot_endpoint, data=json.dumps(appliance_json))
        if response.status_code < 400:
            logger.debug('Sucessfully created appliance with id: {}'.format(appliance_id))
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
            'tags': tags,
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

        log_url = connexion.request.base_url + '/' + run_id
        return {'run_id': run_id, 'run_log': log_url}

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
        logger.debug('_get_stored_run_status, status: {}'.format(status))
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
            #logger.debug('appliance not found')
            return ('P_NOEXIST', None)
        elif resp.status_code > 400:
            logger.error('error querying for appliance')
            return (None, None)
        else:
            data = json.loads(resp.content.decode('utf-8'))
            toil_launcher = [c for c in data['containers'] if c['id'] == 'toil-launcher']
            if len(toil_launcher) == 0:
                logger.warn('toil launcher not found in appliance for run_id: {}'.format(run['run_id']))
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
        logger.info('_get_pivot_run_status, status: {}'.format(status))
        return ('P_EXIST', status)

    """
    Returns:
        - (None, None): error
        - (P_EXIST, ...): appliance still exists
        - (P_NOEXIST, ...): appliance does not exist
        - (..., str status): status of the run
    """
    def _update_run_status(self, run_id, json_path):
#        run_json = '/toil-intermediate/wes/' + run_id + '/run.json'
        logger.info('_update_run_status for run_id: {}'.format(run_id))
        with open(json_path) as fin:
            run = json.load(fin)

        stored_status = self._get_stored_run_status(run)
        if stored_status not in ['COMPLETE', 'CANCELED']:
            logger.debug('querying pivot for status for run_id: {}'.format(run_id))
            (pivot_exist, pivot_status) = self._get_pivot_run_status(run)
        else:
            logger.info('skipping query to pivot for run_id: {}, run is marked {}'.format(run_id, stored_status))
            # don't do whole query to pivot if run has finished
            # we know that the appliance was deleted so set P_NOEXIST
            (pivot_exist, pivot_status) = ('P_NOEXIST', stored_status)

        need_to_update_file = False
        # got a status from pivot and it didn't match stored one
        if pivot_status and pivot_status != stored_status:
            logger.debug('updating status of run [{}] to [{}]'.format(run_id, pivot_status))
            run['status'] = pivot_status
            need_to_update_file = True
        # got stored status but none from pivot, because skipped query or appliance was gone
        elif pivot_exist == 'P_NOEXIST' and stored_status:
            if stored_status in ['RUNNING', 'QUEUED', 'INITIALIZING']:
                logger.info('run is marked as not finished, but appliance doesn\'t exist, updating to canceled')
                run['status'] = 'CANCELED'
                need_to_update_file = True
            else:
                run['status'] = stored_status

        if need_to_update_file:
            logger.info('updating run file for run_id: {}'.format(run_id))
            with open(json_path, 'w') as fout:
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
        stdout_path = ''
        statedir = self._get_statedir(run_id)
        if not statedir:
            return {
                'msg': 'workflow not found',
                'status_code': 404}, 404
        stdout_path = statedir + '/wes/' + run_id + '/stdout.txt'
        run_json = statedir + '/wes/' + run_id + '/run.json'
        if not (os.path.exists(stdout_path) and os.path.exists(run_json)):
            return {'msg': 'Run with id {} not found'.format(run_id), 'status_code': 404}, 404

        #TODO careful with this data size, could be large
        MAX_STDOUT_LEN = 2000 # lines of stdout to return
        with open(stdout_path) as stdout:
            out_data = stdout.read()
            out_data = '\n'.join(out_data.rsplit('\n', MAX_STDOUT_LEN)[1:])

        run_status, app_status = self._update_run_status(run_id, run_json)
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
        logger.debug('cancel run')
        appliance_name = 'wes-workflow-' + run_id
        url = self.pivot_endpoint + '/' + appliance_name
        logger.debug('issuing delete to ' + url)
        response = requests.delete(url)
        if response.status_code == 404 or response.status_code < 400:
            statedir = self._get_statedir(run_id)
            if not statedir:
                return {
                    'status_code': 404,
                    'msg': 'Run with id {} not found'.format(run_id)
                }, 404
            run_json = statedir + '/wes/' + run_id + '/run.json'
            logger.info('CancelRun updating status of {} to CANCELED'.format(run_id))
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
        statedir = self._get_statedir(run_id)
        if not statedir:
            return {
                'msg': 'workflow not found',
                'status_code': 404}, 404
        run_json = statedir + '/wes/' + run_id + '/run.json'
        if os.path.exists(run_json):
            state, _ = self._update_run_status(run_id, run_json)
            return {
                'workflow_id': run_id,
                'state': state
            }
        return {
            'msg': 'The requested Workflow wasn\'t found',
            'status_code': 404
        }, 404


def create_backend(app, opts):
    return PivotBackend(opts)
