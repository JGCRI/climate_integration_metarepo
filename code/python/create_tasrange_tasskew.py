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
import glob
import sys

import numpy as np
import pandas as pd
import xarray as xr


def create_tasrange_tasskew_stitched(run_details):
    # List of all models and scenarios being used
    scenarios = np.unique(run_details.Scenario.values)
    esms = np.unique(run_details.ESM.values)

    for esm in esms:
        for scenario in scenarios:
            # Get input location
            current_task = run_details[
                (run_details['ESM'] == esm) &
                (run_details['Scenario'] == scenario)
            ]
            esm_input_location = current_task['ESM_Input_Location'].values[0]

            # Does tas, tasmin and tasmax exist?
            try:
                tas_files = glob.glob(os.path.join(esm_input_location, f'stitched_{esm}_tas_{scenario}.nc'))
                tasmax_files = glob.glob(os.path.join(esm_input_location, f'stitched_{esm}_tasmax_{scenario}.nc'))
                tasmin_files = glob.glob(os.path.join(esm_input_location, f'stitched_{esm}_tasmin_{scenario}.nc'))
                assert len(tas_files) != 0, 'No tas files'
                assert len(tasmax_files) != 0, 'No tasmax files'
                assert len(tasmin_files) != 0, 'No tasmin files'
            except AssertionError:
                next

            # Open data
            tas_data = xr.open_mfdataset(tas_files)
            tasmin_data = xr.open_mfdataset(tasmin_files)
            tasmax_data = xr.open_mfdataset(tasmax_files)

            # Create tasrange
            tasrange_array = tasmax_data['tasmax'] - tasmin_data['tasmin']
            # Create tasskew
            tasskew_array = (tas_data['tas'] - tasmin_data['tasmin']) / tasrange_array

            # Convert to xarray Dataset from Dataarray
            tasrange_data = tasrange_array.to_dataset(name='tasrange')
            tasskew_data = tasskew_array.to_dataset(name='tasskew')

            # If tasrange files don't already exist, create them
            try:
                tasrange_files = glob.glob(os.path.join(esm_input_location, f'stitched_{esm}_tasrange_{scenario}.nc'))
                assert len(tasrange_files) == 0, 'tasrange files already exist'
                tasrange_array.to_netcdf(os.path.join(esm_input_location, f'stitched_{esm}_tasrange_{scenario}.nc'), compute=True)
            except AssertionError:
                pass

            # If tasskew files don't already exist, create them
            try:
                tasskew_files = glob.glob(os.path.join(esm_input_location, f'stitched_{esm}_tasskew_{scenario}.nc'))
                assert len(tasskew_files) == 0, 'tasskew files already exist'
                tasskew_array.to_netcdf(os.path.join(esm_input_location, f'stitched_{esm}_tasskew_{scenario}.nc'), compute=True)
            except AssertionError:
                pass
            ...
        ...

def create_tasrange_tasskew_CMIP(run_details):
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



if __name__ == "__main__":

# Read in run details ================================================================
    # Input path provided from command line
    run_directory = str(sys.argv[1])
    input_path = os.path.join('intermediate', run_directory)

    # Read in .csv
    run_details = pd.read_csv(os.path.join(input_path, 'run_manage_explicit_list.csv'))

# Get tasks asking for either tasmin or tasmax ======================================
    run_details = run_details[(run_details['Variable'] == 'tasrange') | (run_details['Variable'] == 'tasskew')].copy()

# Get the available models, scenarios, and ensembles (if available)
    if run_details.iloc[0].stitched:
        run_details = run_details[['ESM', 'Scenario']].copy().drop_duplicates()
        create_tasrange_tasskew_stitched(run_details)
    else:
        run_details = run_details[['ESM', 'Scenario', 'Ensemble']].copy().drop_duplicates()
        create_tasrange_tasskew_CMIP(run_details)
