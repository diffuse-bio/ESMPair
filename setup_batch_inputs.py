from argparse import ArgumentParser
import os
import subprocess
from google.cloud import storage


def setup_batch_inputs(input_txt: str, batch_size: int, folder: str):
    # split input_txt into batches of size batch_size and download paralog.a3m files from gcs
    # keeps batches in their own folders
    
    storage_client = storage.Client()
    bucket_path = "diffuse-us-central1-west1"
    bucket = storage_client.bucket(bucket_path)

    with open(input_txt, 'r') as f:
        input_ppis = f.readlines()
        input_ppis = [i.strip().split('\t') for i in input_ppis]
    
    # distribute PPIs into batches (each batch is a new directory), and pulls each pair of paralog.a3m files to a subfolder inside the batch
    os.mkdir(f'{folder}/esmpair_out')
    c = 0
    new_path = ''
    for id_A, id_B in input_ppis:
        if c % batch_size == 0:
            # {os.path.dirname(ppi_data)}
            subdir_path = f'{folder}/esmpair_out/{c//batch_size}/'
            os.mkdir(subdir_path)
        
        blob_A = bucket.blob(f'data/msas/server_msas/single_msa_tax/{id_A}.paralog.a3m')
        blob_B = bucket.blob(f'data/msas/server_msas/single_msa_tax/{id_B}.paralog.a3m')
        if blob_A.exists() and blob_B.exists():
            # copy these blobs to subdir_path
            subfolder_path = f'{subdir_path}/{id_A}_{id_B}'
            os.mkdir(subfolder_path)
            with open(f'{subfolder_path}/{id_A}.paralog.a3m', 'w') as f:
                f.write(blob_A.open('r').read())
            with open(f'{subfolder_path}/{id_B}.paralog.a3m', 'w') as f:
                f.write(blob_B.open('r').read())

        c += 1


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "ppi_data",
        type=str,
        help="Path to txt file of ppi pairs",
    )
    parser.add_argument(
        "batch_size",
        type=int,
        help="Batch size",
    )
    parser.add_argument(
        "tmp_dir",
        type=str,
        help="tmp dir",
    )

    args = parser.parse_args()
    setup_batch_inputs(args.ppi_data, args.batch_size, args.tmp_dir)


if __name__ == '__main__':
    main()