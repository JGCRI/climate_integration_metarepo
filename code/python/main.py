"""
This file manages which scripts are used for each job
"""

from pangeo import basd_pangeo
from downloaded import basd_downloaded

import os
import socket
import sys

from dask.distributed import (Client, LocalCluster)
import numpy as np
import pandas as pd

if __name__ == "__main__":

# Paths =======================================================================================================
    intermediate_path = 'intermediate'

# Get Run Details =============================================================================================

    # Task index from SLURM array to run specific variable and model combinations
    task_id = int(sys.argv[1])
    # Task list csv
    task_list = str(sys.argv[2])
    # Extract task details
    task_details = pd.read_csv(os.path.join(intermediate_path, task_list)).iloc[task_id]

# Check if using Pangeo =======================================================================================

    # Boolean will be true when no input location is given
    using_pangeo = pd.isna(task_details.ESM_Input_Location)

    with LocalCluster(processes=True, threads_per_worker=1) as cluster, Client(cluster) as client:
        # Setting up dask.Client so that I can ssh into the dashboard
        port = client.scheduler_info()['services']['dashboard']
        host = client.run_on_scheduler(socket.gethostname)
        print("If running remotely use the below command to ssh into dashboard")
        print(f"ssh -N -L 8000:{host}:{port} <username>@<remote name>", flush=True)
        print("If running locally, just visit the below link")
        print({client.dashboard_link})

        if using_pangeo:
            # Run pangeo script
            basd_pangeo(task_details)
        else:
            # Run downloaded data script
            basd_downloaded(task_details)

        client.close()
        cluster.close()
