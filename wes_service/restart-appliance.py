import json
import requests
import sys

with open('pivot-restart.json') as fin:
    data = json.load(fin)

run_id = sys.argv[1]
data['id'] = 'wes-workflow-' + run_id
args = [ "_toil_exec",
    "cwltoil --restart --no-match-user --clean onSuccess --cleanWorkDir onSuccess --linkImports --not-strict --batchSystem chronos --defaultCores 16 --defaultDisk 30G --defaultMemory 32G --workDir /toil-intermediate --outdir /toil-intermediate/wes_output --jobStore /toil-intermediate/jobstore-wes-{id} https://raw.githubusercontent.com/DataBiosphere/topmed-workflows/1.24.0/aligner/sbg-alignment-cwl/topmed-alignment.cwl /toil-intermediate/wes/{id}/joborder.json 2>&1 | tee -a /toil-intermediate/wes/{id}/stdout.txt".format(id=run_id)
]
toil_launcher = [x for x in data['containers'] if x['id'] == 'toil-launcher'][0]
toil_launcher['args'] = args
pivot_endpoint = "http://xxxx:9191/appliance"
pivot_endpoint = sys.argv[1]
resp = requests.post(pivot_endpoint, headers={"Content-Type": "application/json"}, json=data)
print(resp)
print(resp.content.decode('utf-8'))
