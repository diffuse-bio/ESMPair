import shutil
import subprocess as sp
import logging
import os
import glob

_GCS_BUCKET = "diffuse-us-central1-west1"

class MSAUploadManager:
    """
    Uploads MSA .a3m file to folder on GCS

    TODO: check if file already exists, throw an error

    """

    def __init__(
        self, 
        folder: str, # folder with a3m
        gcs_path: str = 'data/msas/server_msas/intermediate_store',
        delete_on_exit: bool = True
    ) -> None:

        # gcs path is a function of the obj type (good to have these stored separately)
        self.gcs_path = gcs_path
        # self.seq_id = seq_id
        # self.seq_batch_id = seq_batch_id
        self.folder = folder

        self.delete_on_exit = delete_on_exit

    
    # def upload(self, seq_id: str, seq_batch_id: int):
    #     # TODO -- implement check if file exists

        
    #     sp.check_call(
    #             f"""
    #             gsutil -m -q cp -r {self.folder}/{seq_batch_id}.a3m gs://{_GCS_BUCKET}/{self.gcs_path}/{seq_id}/{seq_id}.a3m 
    #         """,
    #             shell=True,
    #         )
    #     if self.delete_on_exit: #(self.remote_pdb_obj_exists and not self.local) and self.delete_on_exit:
    #         shutil.rmtree(self.folder)
    #     else:
    #         logging.warning('NOT deleting files locally -- might lead to running out of storage on worker nodes')


    #     self.id_idx = 0
    #     self.batch_files = 0

    # def update(self, seq_id: str, seq_batch_id: int):

    #     # mv 
    #     os.system(f"cp {self.folder}/{seq_batch_id}.a3m {self.folder}/{seq_id}.a3m")
    #     logging.warning(f"moved {self.folder}/{seq_batch_id}.a3m to {self.folder}/{seq_id}.a3m")
        

    def batch_upload(self, depth=0, *args, **kwargs):
        print(f'Looking for output files in {self.folder}...')
        # # on exit -- upload PDB from tmp folder *IF* pdb is not on GCS -- otherwise just download from GCS
        json_files = glob.glob(f"{self.folder}/*.json")
        if len(json_files) == 0:
            logging.warning('NO ajson created -- skipping upload')
        else:
            assert depth == 0
            if len(json_files) > 0:
                sp.check_call(
                    f"""
                    gsutil -m -q cp -r  {self.folder}/*.json gs://{_GCS_BUCKET}/{self.gcs_path}/
                """,
                    shell=True,
                )

        # if self.delete_on_exit: #(self.remote_pdb_obj_exists and not self.local) and self.delete_on_exit:
        #     logging.warning('Deleting files locally')
        #     shutil.rmtree(self.folder)
        # else:
        #     logging.warning('NOT deleting files locally -- might lead to running out of storage on worker nodes')



if __name__ == '__main__':
    msa_upload_mgr = MSAUploadManager(folder='single_protein/0').upload(seq_id="P00452", seq_batch_id=0)