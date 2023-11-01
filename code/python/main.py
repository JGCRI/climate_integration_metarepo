"""
This file manages which scripts are used for each job
"""

from pangeo import basd_pangeo
from downloaded import basd_downloaded
from stitched import basd_stitches

import os
import socket
import sys

import dask
from dask.distributed import (Client, LocalCluster)
import numpy as np
import pandas as pd
import warnings

if __name__ == "__main__":

# Ignore non-helpful warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings('ignore', category=FutureWarning)

# Paths =======================================================================================================
    intermediate_path = 'intermediate'
    input_path = 'input'

# Get Run Details =============================================================================================

    # Task index from SLURM array to run specific variable and model combinations
    task_id = int(sys.argv[1])
    # Name of run directory
    run_name = str(sys.argv[2])
    # Extract task details
    task_details = pd.read_csv(os.path.join(intermediate_path, run_name, 'run_manager_explicit_list.csv')).iloc[task_id]
    # Extract Dask settings
    dask_settings = pd.read_csv(os.path.join(input_path, run_name, 'dask_parameters.csv')).iloc[0]

# Check if using Pangeo =======================================================================================

    # Boolean will be true when no input location is given
    using_pangeo = pd.isna(task_details.ESM_Input_Location) & ~(task_details.Variable in ['tasrange', 'tasskew'])
    # Boolean will be true when using STITCHED data
    using_stitches = task_details.stitched
    # When trying to use pangeo for tasrange/tasskew, data will actually be saved in intermediate
    if pd.isna(task_details.ESM_Input_Location) & (task_details.Variable in ['tasrange', 'tasskew']):
        task_details.ESM_Input_Location = os.path.join(intermediate_path, run_name, 'tasrange_tasskew')



    # Check to see if a non-default dask temporary directory is requested
    # If so, set it using dask config
    if not pd.isna(dask_settings.dask_temp_directory):
        dask.config.set({'temporary_directory': f'{dask_settings.dask_temp_directory}'})

    # Writing task details to log
    print(f'======================================================', flush=True)
    print(f'Task Details:', flush=True)
    print(f'ESM: {task_details.ESM}', flush=True)
    print(f'Variable: {task_details.Variable}', flush=True)
    print(f'Scenario: {task_details.Scenario}', flush=True)
    try:
        print(f'Ensemble Member: {task_details.Ensemble}', flush=True)
    except AttributeError:
        pass
    print(f'Reference Period: {task_details.target_period}', flush=True)
    print(f'Application Period: {task_details.application_period}', flush=True)
    if using_pangeo:
        print('Getting Data From Pangeo', flush=True)
    elif using_stitches:
        print('Using STITCHED Data', flush=True)
    else: 
        print(f'Retrieving Data From {task_details.Reference_Input_Location}', flush=True)
    print(f'======================================================')

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
            basd_pangeo(task_details, run_name)
        elif using_stitches:
            # Run stitches script
            basd_stitches(task_details, run_name)
        else:
            # Run downloaded data script
            basd_downloaded(task_details, run_name)

        client.close()
        cluster.close()
