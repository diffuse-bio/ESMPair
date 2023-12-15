from argparse import ArgumentParser
import sys
import glob
from setup_single_msa import setup_single_msa
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
import utils
from google.cloud import storage

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
Script that does the following:

1) Takes as input list of PPIs (ID_A//Seq_A//ID_B//Seq_B)
2) Downloads the respsective paralog.a3m files from gcs if found
3) Split these across NUM_WORKERS (e.g. if I have 100 workers, split single seq MSA computation between them)
4) Each worker will run run_esmpair.py
5) Upload data to GCS (Folder --> )


To run on Google Batch:
    python compute_esmpair_batch.py 0 1 -i A.paralog.a3m B.paralog.a3m

    This will return a google batch command to submit your jobs

    # gcloud batch jobs submit compute-single-msa-JOB-NAME --location us-central1 --config batch/compute_single_msa_batch.json


TODOs:
-- modify this script for ESMPair
-- create Docker image that all dependencies installed X
-- script to tar + upload workspace
-- create google batch script <==
-- debug locally on our Docker image


1) download Docker image gcr.io/diffuse-370214/esmpair:v0.0
2) run docker image in interactive mode:
    sudo docker run -it gcr.io/diffuse-370214/diffuse-mmseqs-server:v0.3
3) test locally
    source ~/.bashrc &&  source /google-cloud-sdk/path.bash.inc && source /google-cloud-sdk/completion.bash.inc &&   export CLOUDSDK_PYTHON=/root/miniconda3/bin/python

    python compute_esmpair_batch.py 0 1 -i A.paralog.a3m B.paralog.a3m
"""

PATH_TO_DB = "/mnt/disks/colabfold-dbs/" #uniref30_2302 #/mnt/sdc/uniref30_2103/"
PATH_TO_DOCKER_IMG = "gcr.io/diffuse-370214/diffuse-mmseqs-server:v0.3" #gcr.io/diffuse-370214/diffuse-mmseqs:0.1"
DEBUG = False

def compute_single_msas_batch(worker_idx: int, num_workers: int, input_txt: str, batch_size: int, overwrite=False, threshold=10000):
    # look at what already exists on gcs
    storage_client = storage.Client()
    gcs_bucket = "diffuse-us-central1-west1"
    gcs_path = "data/msas/server_msas/single_msa_tax"
    blobs = storage_client.list_blobs(gcs_bucket, prefix=gcs_path, delimiter='*.a3m.tax')
    
    existing_ids = [b.name.split('/')[-1].split('.')[0].strip() for b in blobs]
    print(f'{len(existing_ids)} aln files already at gs://{gcs_bucket}/{gcs_path}')
    
    # read input txt
    input_fasta = utils.parse_fasta(open(input_txt,'r').read())
    
    # apply filters
    new_proteins = {input_fasta[1][i]:input_fasta[0][i] for i in range(len(input_fasta[1])) if input_fasta[1][i] not in existing_ids}
    threshold_new_proteins = {k:v for k,v in new_proteins.items() if len(v) < threshold}
    logging.warning(f'Out of {len(input_fasta[0])} ids in {input_txt}, {len(set(input_fasta[1]))} are unique. {len(new_proteins)} entries do not already exist, and of these, {len(threshold_new_proteins)} are under the threshold of {threshold} aas.')
    
    if not overwrite:
        input_txt = 'tmp_input.fasta'
        utils.to_fasta(list(threshold_new_proteins.values()), list(threshold_new_proteins.keys()), input_txt)
        logging.warning(f'NOT overwriting existing entries.')
    if overwrite:
        logging.warning(f'OVERWRITING existing entries')
            
    if not DEBUG:
        # with tempfile.TemporaryDirectory(delete=False) as tmpdirname:
        tmp_dir = tempfile.mkdtemp() 
        # tmp_dirname = tmp_dir.name
        #  save unique single proteins to a combined fasta file, save lookup index, and save unique single proteins to separate fasta files under new folders   
        lookup_idx_dict = setup_single_msa(input_txt, batch_size, folder=tmp_dir)
        if not overwrite:
            os.remove(input_txt)
    else:
        tmp_dir = './'
        # lookup_idx_dict = setup_single_msa(input_txt, batch_size)
        # pickle.dump(lookup_idx_dict, open('lookup_idx_dict.pkl', 'wb'))

        # sys.exit(0)
        lookup_idx_dict =  pickle.load(open('lookup_idx_dict.pkl', 'rb'))

    seq_batch_folders = glob.glob(f'{tmp_dir}/single_protein/*')
    print(seq_batch_folders)
    logging.warning(f'Logging to {tmp_dir}')

    # divide seq batch folders across workers (this worker will just operate on a subset of the folders)


    k = math.ceil(len(seq_batch_folders) / (num_workers))
    # print(k)
    subset_seq_batch_folders = seq_batch_folders[k * worker_idx: k * (worker_idx +1)]
    # print(k * worker_idx, k * (worker_idx +1))
    print(subset_seq_batch_folders)


    for batch_folder in subset_seq_batch_folders:
        print(f'Batch number: {batch_folder}')

        msa_upload_mgr = MSAUploadManager(folder=batch_folder, gcs_path=gcs_path)
        batch_idx = int(batch_folder[batch_folder.rfind('/')+1:])

        if not DEBUG:
            # run MSA computation in a subprocess -- not very clean, but quick and dirty
            sp.call(f"python3 server_search_monomer.py {batch_folder}/query.fasta {batch_folder}", shell=True)
        
        msa_upload_mgr.batch_upload()
        
        # clear mmseqs server cache
        shutil.rmtree("/ColabFold/MsaServer/jobs")
        os.mkdir("/ColabFold/MsaServer/jobs")
        logging.warning(f'Clearing the MMseqs server cache!')

        # map computed MSAs to corresponding seq IDs --> upload all data for this batch to GCS
        # a3m_files = glob.glob(f"{batch_folder}/*.a3m")
        # for a3m_file in a3m_files:
        #     within_batch_idx = int(a3m_file[a3m_file.rfind('/')+1:-4])
        #     print(within_batch_idx, a3m_file)
        # for within_batch_idx in range(batch_size):
            # try:
            # seq_id = lookup_idx_dict[(batch_idx, within_batch_idx)]
            # msa_upload_mgr.upload(seq_id=seq_id, seq_batch_id=within_batch_idx)
            # except:
            #     logging.warning(f"issue with {batch_folder}/{within_batch_idx}")
        

    if not DEBUG:
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
                    "volumes": [
                        {{
                            "deviceName": "colabfold-dbs",
                            "mountPath": "/mnt/disks/colabfold-dbs",
                            "mountOptions": "ro,noload"
                        }}
                    ],
                    "computeResource": {{
                        "cpuMilli": 7500,
                        "memoryMib": 29500
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
                        "machineType": "n1-standard-8",
                        "provisioningModel": "SPOT",
                        "disks": [
                            {{
                                "deviceName": "colabfold-dbs",
                                "existingDisk": "projects/diffuse-370214/zones/us-central1-a/disks/colabfold-dbs"
                            }}
                        ]
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
    batch_json_path = os.path.join('tmp_batch', f"compute_msas_server_{random_str}.json")
    with open(batch_json_path, "w") as f:
        f.write(batch_file)

    gbatch_cmd = f"gcloud batch jobs submit compute-server-mmseqs-msas-{random_str} --location us-central1 --config {batch_json_path}"
    if tofile:
        with open('gbatch.sh', 'a+') as f:
            f.write(f'{gbatch_cmd}\n')
    else:
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
    parser.add_argument(
        "--overwrite",
        action='store_true',
        help="Whether to overwrite protein IDs that have already been computed and saved to GCS",
    )
    parser.add_argument(
        "--tofile",
        action='store_true',
        help="Save command to file instead of printing to stdout",
    )


    args = parser.parse_args()
    if args.batch:
        return_batch_json(num_workers=args.num_workers, input_txt=args.input, batch_size=args.batch_size, tofile=args.tofile)
    else:
        compute_single_msas_batch(args.worker_idx, num_workers=args.num_workers, input_txt=args.input, batch_size=args.batch_size)
    
