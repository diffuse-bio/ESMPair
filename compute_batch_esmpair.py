from argparse import ArgumentParser
import sys
import glob
from setup_batch_inputs import setup_batch_inputs
import math
import subprocess as sp
import tempfile
from msa_upload_manager import MSAUploadManager
import pickle
import shutil
import string
import random
import os
import upload_workspace
from google.cloud import storage

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
Script that does the following:

1) Takes as input list of PPIs (ID_A\tID_B)
2) Split these pairs across NUM_WORKERS (e.g. if I have 100 workers, split computation between them)
4) Each worker will run ESMPair
5) Upload data to GCS (Folder --> id.a3m)

***NOTE that each worker will have a read-only disk attached with the U30 db***

To run on Google Batch:
    python compute_batch_esmpair.py 0 1 -i test_input.txt --batch

    This will return a google batch command to submit your jobs

    # gcloud batch jobs submit compute-single-msa-JOB-NAME --location us-central1 --config batch/compute_single_msa_batch.json

TODOs:
-- create this script
-- test docker image in isolation
-- test docker image locally. you can run all of the commands individually
-- test submitting to gbatch

2) download Docker image gcr.io/diffuse-370214/gcr.io/diffuse-370214/esmpair:v0.2
3) run docker image in interactive mode
    sudo docker -it gcr.io/diffuse-370214/gcr.io/diffuse-370214/esmpair:v0.2
4) test locally
    source ~/.bashrc &&  source /google-cloud-sdk/path.bash.inc && source /google-cloud-sdk/completion.bash.inc &&   export CLOUDSDK_PYTHON=/root/miniconda3/bin/python

    cd ESMPair
    python run_esmpair.py ...
    

"""

# PATH_TO_DB = "/mnt/disks/colabfold-dbs/" #uniref30_2302 #/mnt/sdc/uniref30_2103/"
PATH_TO_DOCKER_IMG = "gcr.io/diffuse-370214/diffuse-mmseqs-server:v0.5" #gcr.io/diffuse-370214/diffuse-mmseqs:0.1"

def compute_single_esmpair(worker_idx: int, num_workers: int, input_txt: str, batch_size: int, overwrite=False, threshold=10000):
    
    # get input files from GCS; input files are paralog.a3ms 
    storage_client = storage.Client()
    bucket_path = "diffuse-us-central1-west1"
    bucket = storage_client.bucket(bucket_path)
        
    # read input txt
    with open(input_txt, 'r') as f:
        input_ppis = f.readlines()
        input_ppis = [i.strip().split('\t') for i in input_ppis]
    
    # filter out PPIs that do not have paralogs
    possible_ppis = []
    for id_A, id_B in input_ppis:
        blob_A = bucket.blob(f'data/msas/server_msas/single_msa_tax/{id_A}.paralog.a3m')
        blob_B = bucket.blob(f'data/msas/server_msas/single_msa_tax/{id_B}.paralog.a3m')
        if blob_A.exists() and blob_B.exists():
            possible_ppis += [f'{id_A}\t{id_B}\n']
                
    input_txt = 'tmp_input.txt'
    with open(input_txt, 'w') as f:
        f.write(''.join(possible_ppis))
    
            
    # with tempfile.TemporaryDirectory(delete=False) as tmpdirname:
    tmp_dir = tempfile.mkdtemp() 
    # tmp_dirname = tmp_dir.name
    #  save unique PPIs to separate txt files under new folders   
    lookup_idx_dict = setup_batch_inputs(input_txt, batch_size, folder=tmp_dir)
    os.remove(input_txt)

    seq_batch_folders = glob.glob(f'{tmp_dir}/esmpair_out/*')
    print(seq_batch_folders)
    logging.warning(f'Logging to {tmp_dir}')

    # divide seq batch folders across workers (this worker will just operate on a subset of the folders)
    k = math.ceil(len(seq_batch_folders) / (num_workers))
    subset_seq_batch_folders = seq_batch_folders[k * worker_idx: k * (worker_idx +1)]
    print(subset_seq_batch_folders)

    for batch_folder in subset_seq_batch_folders:
        print(f'Batch number: {batch_folder}')

        msa_upload_mgr = MSAUploadManager(folder=batch_folder, gcs_path='data/msas/server_msas/esmpair/')
        batch_idx = int(batch_folder[batch_folder.rfind('/')+1:])
        sp.call(f"python3 run_esmpair.py {batch_folder}", shell=True)
        msa_upload_mgr.batch_upload()
        shutil.rmtree(tmp_dir)
        

def return_batch_json(num_workers: int, input_txt: str, batch_size: int, tofile: bool): #, num_workers: List[int]):
    # get batch script to run parallel jobs
    
    # upload workspace
    letters = string.ascii_lowercase
    random_str = ''.join(random.choice(letters) for i in range(10))
    unique_run_name = "%s-%s" %(os.environ['USER'], random_str)
    print(unique_run_name)
    workspace = upload_workspace.setup_and_upload_workspace(unique_run_name)


    BATCH_JSON = """{{
        "taskGroups": [
            {{
                "taskSpec": {{
                    "runnables": [
                        {{
                            "container": {{
                                "imageUri": "{path_to_docker_img}",
                                "entrypoint": "/bin/bash",
                                "commands": ["-c",
                                            " source ~/.bashrc &&  source /google-cloud-sdk/path.bash.inc && source /google-cloud-sdk/completion.bash.inc &&   export CLOUDSDK_PYTHON=/root/miniconda3/bin/python && gsutil -m cp gs://{gcs_bucket}/workspaces/{workspace}    . && tar -xvzf {workspace}  &&  bash paired_msa/msa_startup.sh  &&  cd paired_msa  &&  python /paired_msa/compute_msas_server.py  $BATCH_TASK_INDEX $BATCH_TASK_COUNT -i {txt} -b {batch_size}"
                                ]
                                

                            }}
                        }}
                        
                    ],
                    "computeResource": {{
                        "cpuMilli": 63500,
                        "memoryMib": 239500
                    }},
                    "maxRetryCount": 1
                }},
                "taskCount": {num_workers},
                "parallelism": {num_workers}
            }}
        ],
        "logsPolicy": {{
            "destination": "CLOUD_LOGGING"
        }},
        "allocationPolicy": {{
            "network": {{
                "networkInterfaces": [
                {{
                    "network": "projects/diffuse-370214/global/networks/default",
                    "subnetwork": "projects/diffuse-370214/regions/us-central1/subnetworks/default",
                    "noExternalIpAddress": true
                }}
                ]
            }},
            "instances": [
                {{
                    "policy": {{
                        "machineType": "n1-standard-64",
                        "provisioningModel": "SPOT",
                    }}
                }}
            ],
            "location": {{
                "allowedLocations": [
                    "zones/us-central1-a"
                ]
            }}
        }}
    }}"""

    # for txt, nw in txt_files_and_num_workers:
    batch_file = BATCH_JSON.format(gcs_bucket=upload_workspace._GCS_BUCKET, path_to_docker_img=PATH_TO_DOCKER_IMG, txt=input_txt, workspace=workspace, num_workers=num_workers, batch_size=batch_size)
    random_str = ''.join(random.choice(letters) for i in range(10))
    os.system('mkdir -p tmp_batch')
    batch_json_path = os.path.join('tmp_batch', f"compute_esmpair{random_str}.json")
    with open(batch_json_path, "w") as f:
        f.write(batch_file)

    gbatch_cmd = f"gcloud batch jobs submit compute-esmpair-{random_str} --location us-central1 --config {batch_json_path}"
    logger.info(f'Run: {gbatch_cmd}')



if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(
        "worker_idx",
        type=int,
        help="Index of worker",
    )
    parser.add_argument(
        "num_workers",
        type=int,
        help="Total number of parallel workers",
    )
    parser.add_argument('-i', '--input', type=str, help="Input list of PPIs (ID_A//Seq_A//ID_B//Seq_B)")  
    parser.add_argument('-b', '--batch_size', type=int, default=16, help="Batch size for mmseqs (total number of MSAs to compute in parallel)")  
    parser.add_argument(
        "--batch",
        action='store_true',
        help="Whether to return Google Batch script",
    )
    
    args = parser.parse_args()
    if args.batch:
        return_batch_json(num_workers=args.num_workers, input_txt=args.input, batch_size=args.batch_size)
    else:
        compute_single_esmpair(args.worker_idx, num_workers=args.num_workers, input_txt=args.input, batch_size=args.batch_size)
    
