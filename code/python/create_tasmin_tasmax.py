"""
Description: This script will look at the input run manager file to determine for what data the user wants
             tasmin and tasmax output. It will then look for each of those run's respective tas, tasrange and tasskew
             output, and create the files accordingly.
Author: Noah Prime
Modified: August 7, 2023
Input:
    - input/<run manager>.csv - file that specifies all the runs requested
    - input/encoding.csv - settings for encoding output NetCDF
    - input/attributes.csv - attributes to save to NetCDF metadata
    - tas, tasrange, and tasskew output files
Output:
    - tasmin and tasmax output files
"""

# To calculate tasmin and tasmax:
# tasmin = tas - tasskew * tasrange
# tasmax = tasmin + tasrange

# Packages =============================================================================================
import os                                   # For navigating os
import socket
import sys

import dask
import dask.array as da
from dask.distributed import Client, LocalCluster, progress
import numpy as np
import xarray as xr


if __name__ == "__main__":

    # Define paths
    input_files_path = '../../input'
    intermediate_path = '../../intermediate'

    # Read in run manager file
    run_manager_file = str(sys.argv[1])

# Init Constants =============================================================================================

    # Run specs
    # target_model = 'GFDL-ESM4'
    target_model = 'MPI-ESM1-2-HR'
    target_scenario = 'ssp245'
    target_ensemble = 'r1i1p1f1'
    hist_model = 'W5E5v2'
    # Temporal resolution of data
    time_res = 'monthly'
    print("====================================================================", flush=True)
    print("Creating tasmin and tasmax", flush=True)
    print("====================================================================", flush=True)


    # Input (only doing monthly data in this script for now)
    # data_path = '/rcfs/projects/gcims/models/basd/outputs/CanESM5_W5E5v2'
    data_path = f'/rcfs/projects/gcims/models/basd/outputs/{hist_model}/{target_model}/{target_scenario}'
    # Bias Adjusted
    tas_ba_name = os.path.join('ba', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tas_global_{time_res}_2015_2100.nc')
    tasrange_ba_name = os.path.join('ba', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tasrange_global_{time_res}_2015_2100.nc')
    tasskew_ba_name = os.path.join('ba', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tasskew_global_{time_res}_2015_2100.nc')
    # Bias Adjusted and Downscaled
    tas_basd_name = os.path.join('basd', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tas_global_{time_res}_2015_2100.nc')
    tasrange_basd_name = os.path.join('basd', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tasrange_global_{time_res}_2015_2100.nc')
    tasskew_basd_name = os.path.join('basd', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tasskew_global_{time_res}_2015_2100.nc')

    # Output
    # Bias Adjusted
    tasmin_ba_name = os.path.join('ba', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tasmin_global_{time_res}_2015_2100.nc')
    tasmax_ba_name = os.path.join('ba', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tasmax_global_{time_res}_2015_2100.nc')
    # Bias Adjusted and Downscaled
    tasmin_basd_name = os.path.join('basd', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tasmin_global_{time_res}_2015_2100.nc')
    tasmax_basd_name = os.path.join('basd', f'{target_model}_{target_ensemble}_{hist_model}_{target_scenario}_tasmax_global_{time_res}_2015_2100.nc')


# Begin Dask =============================================================================================
    # conduct adjustment in parallel, and save netCDF to output file
    dask.config.set({'temporary_directory': '/scratch/'})
    with LocalCluster(processes=True, threads_per_worker=1) as cluster, Client(cluster) as client:
        # Setting up dask.Client so that I can ssh into the dashboard
        port = client.scheduler_info()['services']['dashboard']
        host = client.run_on_scheduler(socket.gethostname)
        print(f"ssh -N -L 8000:{host}:{port} prim232@deception03.pnl.gov", flush=True)

        # NetCDF encoding
        output_encoding = {'missing_value': 1e+20, '_FillValue': 1e+20}

        # Bias Adjusted Data first
        # Open data
        tas_ba = xr.open_mfdataset(os.path.join(data_path, tas_ba_name), chunks={'time': 100}).transpose('time', 'lat', 'lon')
        tasrange_ba = xr.open_mfdataset(os.path.join(data_path, tasrange_ba_name), chunks={'time': 100}).transpose('time', 'lat', 'lon')
        tasskew_ba = xr.open_mfdataset(os.path.join(data_path, tasskew_ba_name), chunks={'time': 100}).transpose('time', 'lat', 'lon')

        # Create tasmin and tasmax
        mult_data = da.map_blocks(np.multiply, tasskew_ba['tasskew'].data, tasrange_ba['tasrange'].data)
        sub_data = da.map_blocks(np.subtract, tas_ba['tas'].data, mult_data)

        # Create and save tasmin
        tasmin = tas_ba.copy()
        tasmin['tas'].data = sub_data
        tasmin = tasmin.rename(tas = 'tasmin').compute()
        write_job = tasmin[['time', 'lat', 'lon', 'tasmin']].to_netcdf(os.path.join(data_path, tasmin_ba_name), encoding = {'tasmin': output_encoding}, compute=True)
        progress(write_job)
        tasmin.close()

        # Create and save tasmax
        sum_data = da.map_blocks(np.subtract, tasrange_ba['tasrange'].data, sub_data)
        tasmax = tas_ba.copy()
        tasmax['tas'].data = sub_data
        tasmax = tasmax.rename(tas = 'tasmax').compute()
        write_job = tasmax[['time', 'lat', 'lon', 'tasmax']].to_netcdf(os.path.join(data_path, tasmax_ba_name), encoding = {'tasmax': output_encoding}, compute=True)
        progress(write_job)
        tasmax.close()

        # Close
        tas_ba.close()
        tasrange_ba.close()
        tasskew_ba.close()

        # Now Bias Adjusted and Downscaled
        # Open Data
        tas_basd = xr.open_mfdataset(os.path.join(data_path, tas_basd_name), chunks={'time': 100}).transpose('time', 'lat', 'lon')
        tasrange_basd = xr.open_mfdataset(os.path.join(data_path, tasrange_basd_name), chunks={'time': 100}).transpose('time', 'lat', 'lon')
        tasskew_basd = xr.open_mfdataset(os.path.join(data_path, tasskew_basd_name), chunks={'time': 100}).transpose('time', 'lat', 'lon')

        # Create tasmin and tasmax
        mult_data = da.map_blocks(np.multiply, tasskew_basd['tasskew'].data, tasrange_basd['tasrange'].data)
        sub_data = da.map_blocks(np.subtract, tas_basd['tas'].data, mult_data)

        # Create and save tasmin
        tasmin = tas_basd.copy()
        tasmin['tas'].data = sub_data
        tasmin = tasmin.rename(tas = 'tasmin').compute()
        write_job = tasmin[['time', 'lat', 'lon', 'tasmin']].to_netcdf(os.path.join(data_path, tasmin_basd_name), encoding = {'tasmin': output_encoding}, compute=True)
        progress(write_job)
        tasmin.close()

        # Create and save tasmax
        sum_data = da.map_blocks(np.subtract, tasrange_basd['tasrange'].data, sub_data)
        tasmax = tas_basd.copy()
        tasmax['tas'].data = sub_data
        tasmax = tasmax.rename(tas = 'tasmax').compute()
        write_job = tasmax[['time', 'lat', 'lon', 'tasmax']].to_netcdf(os.path.join(data_path, tasmax_basd_name), encoding = {'tasmax': output_encoding}, compute=True)
        progress(write_job)
        tasmax.close()

        # Close
        tas_basd.close()
        tasrange_basd.close()
        tasskew_basd.close()


    print(f'YAY==================================================================================================')