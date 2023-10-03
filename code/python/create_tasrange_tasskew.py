# Script Details =============================================================================================
# Author: Noah Prime
# Last Updated: July 5, 2023
# Purpose: Generates the variables tasrange and tasskew from the variables tas, tasmin and tasmax.
#          Does so for both historic period, and the future period a given scenario.
#          These are use for bias adjustment and statistical downscaling, and then converted
#          back to tasmin, tasmax after that.
# Inputs:  tas, tasmin, tasmax for historic and future periods for given ESM, scenario, ensemble member.
# Outputs: tasrange, tasskew for historic and future periods for given ESM, scenerio, ensemble member.
# Usage: Save the ESM data (tas, tasmin and tasmax) to it's respective directory at
#        /rcfs/projects/gcims/data/climate/cmip6/<ESM name>
#        Update the ESM, scenario and target ensembles at top of this script.
#        Check input and output file name structures. Often will be the same between ESM's, since
#        ISIMP forces some consistency, but there may be some minor changes to make.
#        Then, run > python create_tasskew_tasrange.py
# TODO: Add global and variable attributes to output NetCDF

# Packages =============================================================================================
import os                                   # For navigating os
import socket
import sys

import dask
import numpy as np
import pandas as pd
import xarray as xr

import warnings


if __name__ == "__main__":

# Read in run details ================================================================
    # Input path provided from command line
    run_directory = str(sys.argv[1])
    input_path = os.path.join('intermediate', str(sys.argv[2]))

    # Read in .csv
    run_details = pd.read_csv(os.path.join(input_path, 'run_manage_explicit_list.csv'))

# Get tasks asking for either tasmin or tasmax ======================================
    run_details = run_details[(run_details['Variable'] == 'tasmin') | (run_details['Variable'] == 'tasmax')].copy()

# Get the available models, scenarios, and ensembles (if available)
    if run_details.iloc[0].stitched:
        run_details = run_details[['ESM', 'Scenario']].copy().drop_duplicates()
    else:
        run_details = run_details[['ESM', 'Scenario', 'Ensemble']].copy().drop_duplicates()

# Init Constants =============================================================================================

    # Run specs
    target_model = 'IPSL-CM6A-LR'
    target_scenario = 'ssp245'
    target_ensemble = 'r1i1p1f1'
    print("====================================================================", flush=True)
    print("Creating tasrange and tasskew", flush=True)
    print("====================================================================", flush=True)


    # Output file name (Bias Adjustment)
    output_path = f'/rcfs/projects/gcims/data/climate/cmip6/{target_model}'
    output_tasrange_fut_file_name = f'tasrange_day_{target_model}_{target_scenario}_{target_ensemble}_gn_20150101-21001230.nc'
    output_tasskew_fut_file_name = f'tasskew_day_{target_model}_{target_scenario}_{target_ensemble}_gn_20150101-21001230.nc'
    output_tasrange_hist_file_name = f'tasrange_day_{target_model}_historical_{target_ensemble}_gn_18500101-20141230.nc'
    output_tasskew_hist_file_name = f'tasskew_day_{target_model}_historical_{target_ensemble}_gn_18500101-20141230.nc'


# Obs hist and Parameters object ======================================================================

    # Reading in CMIP6 data
    input_sim_dir = output_path
    tas_sim_fut_pattern = f'tas_day_{target_model}_{target_scenario}_{target_ensemble}_*.nc'
    tas_sim_hist_pattern = f'tas_day_{target_model}_historical_{target_ensemble}_*.nc'
    tasmin_sim_fut_pattern = f'tasmin_day_{target_model}_{target_scenario}_{target_ensemble}_*.nc'
    tasmin_sim_hist_pattern = f'tasmin_day_{target_model}_historical_{target_ensemble}_*.nc'
    tasmax_sim_fut_pattern = f'tasmax_day_{target_model}_{target_scenario}_{target_ensemble}_*.nc'
    tasmax_sim_hist_pattern = f'tasmax_day_{target_model}_historical_{target_ensemble}_*.nc'

# Begin Dask =============================================================================================

    # Open data
    tas_sim_fut = xr.open_mfdataset(os.path.join(input_sim_dir, tas_sim_fut_pattern))
    tasmin_sim_fut = xr.open_mfdataset(os.path.join(input_sim_dir, tasmin_sim_fut_pattern))
    tasmax_sim_fut = xr.open_mfdataset(os.path.join(input_sim_dir, tasmax_sim_fut_pattern))
    tas_sim_hist = xr.open_mfdataset(os.path.join(input_sim_dir, tas_sim_hist_pattern))
    tasmin_sim_hist = xr.open_mfdataset(os.path.join(input_sim_dir, tasmin_sim_hist_pattern))
    tasmax_sim_hist = xr.open_mfdataset(os.path.join(input_sim_dir, tasmax_sim_hist_pattern))

    # Create tasrange
    tasrange_sim_fut = tasmax_sim_fut['tasmax'] - tasmin_sim_fut['tasmin']
    tasrange_sim_hist = tasmax_sim_hist['tasmax'] - tasmin_sim_hist['tasmin']

    # Create tasskew
    tasskew_sim_fut = (tas_sim_fut['tas'] - tasmin_sim_fut['tasmin']) / tasrange_sim_fut
    tasskew_sim_hist = (tas_sim_hist['tas'] - tasmin_sim_hist['tasmin']) / tasrange_sim_hist

    # Convert to xarray Dataset from Dataarray
    tasrange_sim_fut = tasrange_sim_fut.to_dataset(name='tasrange')
    tasrange_sim_hist = tasrange_sim_hist.to_dataset(name='tasrange')
    tasskew_sim_fut = tasskew_sim_fut.to_dataset(name='tasskew')
    tasskew_sim_hist = tasskew_sim_hist.to_dataset(name='tasskew')

    # Write out intermediate files
    # tasrange
    tasrange_sim_fut.to_netcdf(os.path.join(output_path, output_tasrange_fut_file_name), compute=True)
    tasrange_sim_hist.to_netcdf(os.path.join(output_path, output_tasrange_hist_file_name), compute=True)

    # tasskew
    tasskew_sim_fut.to_netcdf(os.path.join(output_path, output_tasskew_fut_file_name), compute=True)
    tasskew_sim_hist.to_netcdf(os.path.join(output_path, output_tasskew_hist_file_name), compute=True)


    print(f'YAY==================================================================================================')