import subprocess as sp
import os
import logging
import string
import random
import sys
sys.path.append('.')
import tempfile
# from util import gcs_util

_GCS_BUCKET = 'diffuse-us-central1-west1'
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


ARCHIVE_COMMAND_TEMPLATE = """
cd '{workspace}';
find -L $(ls --color=none ESMPair|xargs -I @ echo ESMPair/@) \
    ! -iname '*.avi' \
    ! -iname '*.bmp' \
    ! -iname '*.pcd' \
    ! -iname '*.pdb' \
    ! -iname '*.bin' \
    ! -iname '*.DS_Store' \
    ! -iname '*.gz' \
    ! -iname '*.pt' \
    ! -iname '*.pyc' \
    ! -iname '*.ipynb' \
    ! -ipath '*/logs*' \
    ! -ipath '*/.git*' \
    ! -ipath '*/txt*' \
    ! -ipath '*/imgs*' \
    ! -ipath '*/wandb*' \
    ! -ipath '*/webprot*' \
    ! -ipath '*/env*' \
    ! -ipath '*/pkl*' \
    ! -ipath '*/png*' \
    ! -ipath '*/csv*' \
    ! -ipath '*/checking_diffs*' \
    ! -ipath '*.npz' \
    ! -ipath '*.png' \
    ! -ipath '*/google-cloud-sdk*' \
    ! -ipath '*/se3*' \
    ! -ipath '*/pkl*' \
    ! -ipath '*.pt' \
    ! -ipath '*/__pycache__*' \
    ! -ipath '*/heteroligomer_pdb_obj*' \
    ! -ipath '*/sampling_runs*' \
    ! -ipath '*/docker*' \
    ! -ipath '*/venv*' \
    ! -ipath '*/MMseqs*' \
    ! -ipath '*/notebooks*' \
| GZIP=-1 tar czf '{archive}' -T -;
"""

# conda install -y numpy google-cloud-storage biopython;
def upload_workspace(workspace_path_local: str, workspace_path_gcs: str) -> str:
    with tempfile.TemporaryDirectory() as tmpdirname:
        archive_path = os.path.join(tmpdirname, "archive.tar.tz")
        logging.info('gzipping workspace: %s' %archive_path)
        print(workspace_path_local, workspace_path_gcs)
        sp.check_call(
            ARCHIVE_COMMAND_TEMPLATE.format(
                workspace=workspace_path_local,
                archive=archive_path,
            ),
            shell=True,
        )
        sp.check_call(
            """
            gsutil -m cp {archive_path} {gcs_path}
            """.format(
                archive_path=archive_path, gcs_path=workspace_path_gcs
            ),
            shell=True,
        )



def setup_and_upload_workspace(
    unique_run_name: str,
):

    if False:
        assert "_" not in unique_run_name

    current_dir = os.path.split(os.getcwd())
    assert current_dir[-1] == "ESMPair", current_dir

    assert all(e not in unique_run_name for e in ("/", ".", ":")), (
        "No special characters allowed in %s" % unique_run_name
    )
    tar_file_name = "workspace_%s.tar.gz" % unique_run_name    

    gcs_workspace_path = f"gs://{_GCS_BUCKET}/workspaces/%s" % tar_file_name
    print('gcs_workspace_path', gcs_workspace_path)
    upload_workspace(
        workspace_path_local=os.path.join(*current_dir[:-1]),
        workspace_path_gcs=gcs_workspace_path,
    )
    logger.info("Uploaded workspace to %s" % gcs_workspace_path)
    
    return tar_file_name


if __name__ == '__main__':

    letters = string.ascii_lowercase
    random_str = ''.join(random.choice(letters) for i in range(10))
    unique_run_name = "%s-%s" %(os.environ['USER'], random_str)

    setup_and_upload_workspace(unique_run_name)