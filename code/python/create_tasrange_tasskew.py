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

import fsspec  # Used semi-secretly in pangeo
import intake  # Used semi-secretly in pangeo
import numpy as np
import pandas as pd
import xarray as xr
import warnings


def create_tasrange_tasskew_stitched(run_details):
    # List of all models and scenarios being used
    scenarios = np.unique(run_details.Scenario.values)
    esms = np.unique(run_details.ESM.values)

    for esm in esms:
        for scenario in scenarios:
            print(f'Creating tasrange and tasskew for {esm} {scenario}')
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

            # Convert to xarray Dataset from DataArray
            tasrange_data = tasrange_array.to_dataset(name='tasrange')
            tasskew_data = tasskew_array.to_dataset(name='tasskew')

            # If tasrange files don't already exist, create them
            try:
                tasrange_files = glob.glob(os.path.join(esm_input_location, f'stitched_{esm}_tasrange_{scenario}.nc'))
                assert len(tasrange_files) == 0, 'tasrange files already exist'
                tasrange_data.to_netcdf(os.path.join(esm_input_location, f'stitched_{esm}_tasrange_{scenario}.nc'), compute=True)
            except AssertionError:
                print('Warning, tasrange files already exist')
                pass

            # If tasskew files don't already exist, create them
            try:
                tasskew_files = glob.glob(os.path.join(esm_input_location, f'stitched_{esm}_tasskew_{scenario}.nc'))
                assert len(tasskew_files) == 0, 'tasskew files already exist'
                tasskew_data.to_netcdf(os.path.join(esm_input_location, f'stitched_{esm}_tasskew_{scenario}.nc'), compute=True)
            except AssertionError:
                print('Warning, tasskew files already exist')
                pass
            ...
        ...


def create_tasrange_tasskew_CMIP(run_details, output_path):
    # List of all models, scenarios and ensemble members being used
    scenarios = np.append( np.unique(run_details.Scenario.values), ['historical'] )
    esms = np.unique(run_details.ESM.values)
    ensembles = np.unique(run_details.Ensemble.values)

    for esm in esms:
        for scenario in scenarios:
            for ensemble in ensembles:
                print(f'Creating tasrange and tasskew for {esm} {scenario}', flush=True)
                # Get input location
                if scenario == 'historical':
                    current_task = run_details[
                        (run_details['ESM'] == esm) &
                        (run_details['Ensemble'] == ensemble)
                    ]
                else:
                    current_task = run_details[
                        (run_details['ESM'] == esm) &
                        (run_details['Scenario'] == scenario) &
                        (run_details['Ensemble'] == ensemble)
                    ]
                esm_input_location = current_task['ESM_Input_Location'].values[0]
                using_pangeo = pd.isna(esm_input_location)
                if using_pangeo:
                    try:
                        create_tasrange_tasskew_pangeo(output_path, esm, scenario, ensemble)
                    except:
                        raise
                        pass
                    continue

                # Does tas, tasmin and tasmax exist?
                try:
                    tas_files = glob.glob(os.path.join(esm_input_location, f'tas_day_{esm}_{scenario}_{ensemble}_*.nc'))
                    tasmax_files = glob.glob(os.path.join(esm_input_location, f'tasmax_day_{esm}_{scenario}_{ensemble}_*.nc'))
                    tasmin_files = glob.glob(os.path.join(esm_input_location, f'tasmin_day_{esm}_{scenario}_{ensemble}_*.nc'))
                    assert len(tas_files) != 0, 'No tas files'
                    assert len(tasmax_files) != 0, 'No tasmax files'
                    assert len(tasmin_files) != 0, 'No tasmin files'
                except AssertionError:
                    continue

                # Open data
                tas_data = xr.open_mfdataset(tas_files)
                tasmin_data = xr.open_mfdataset(tasmin_files)
                tasmax_data = xr.open_mfdataset(tasmax_files)

                # Create tasrange
                tasrange_array = tasmax_data['tasmax'] - tasmin_data['tasmin']
                # Create tasskew
                tasskew_array = (tas_data['tas'] - tasmin_data['tasmin']) / tasrange_array

                # Convert to xarray Dataset from DataArray
                tasrange_data = tasrange_array.to_dataset(name='tasrange')
                tasskew_data = tasskew_array.to_dataset(name='tasskew')

                # First and last day in data as string for file name
                start_str = np.min(pd.DatetimeIndex(tas_data['time'].values)).strftime('%Y%m%d')
                end_str = np.max(pd.DatetimeIndex(tas_data['time'].values)).strftime('%Y%m%d')

                # If tasrange files don't already exist, create them
                try:
                    tasrange_files = glob.glob(os.path.join(esm_input_location, f'tasrange_day_{esm}_{scenario}_{ensemble}_*.nc'))
                    assert len(tasrange_files) == 0, 'tasrange files already exist'
                    tasrange_data.to_netcdf(os.path.join(esm_input_location, f'tasrange_day_{esm}_{scenario}_{ensemble}_{start_str}-{end_str}.nc'), compute=True)
                except AssertionError:
                    print('Warning, tasrange files already exist')
                    pass

                # If tasskew files don't already exist, create them
                try:
                    tasskew_files = glob.glob(os.path.join(esm_input_location, f'tasskew_day_{esm}_{scenario}_{ensemble}_*.nc'))
                    assert len(tasskew_files) == 0, 'tasskew files already exist'
                    tasskew_data.to_netcdf(os.path.join(esm_input_location, f'tasskew_day_{esm}_{scenario}_{ensemble}_{start_str}-{end_str}.nc'), compute=True)
                except AssertionError:
                    print('Warning, tasskew files already exist')
                    pass

                ...
            ...
        ...
    ...


# Helper function to easily pull a netcdf from pangeo with just the zstore address from
# the stitches `pangeo_table.csv` file copied into this project.
def fetch_nc(zstore, **kwargs):
    """Extract data for a single file.
    :param zstore:                str of the location of the cmip6 data file on pangeo.
    :param chunks:                dictionary of dask chunk specification
    :return:                      an xarray containing cmip6 data downloaded from the pangeo.
    """
    ds = xr.open_zarr(fsspec.get_mapper(zstore), **kwargs)
    # ds.sortby('time')
    return ds


# Update the pangeo table
def fetch_pangeo_table():
    """ Get a copy of the pangeo archive contents
    :return: a pd data frame containing information about the model, source, experiment, ensemble and
    so on that is available for download on pangeo.
    """
    # smaller set of experiments to save to make life easier.
    # experiments most likely to want to pattern scale on or
    # otherwise get data from
    exps =  ['historical', 'ssp370',
            'ssp585', 'ssp126', 'ssp245',
            'ssp119', 'ssp460', 'ssp434',
            'ssp534-over', 'piControl']

    # The url path that contains to the pangeo archive table of contents.
    url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    dat = intake.open_esm_datastore(url)
    dat = dat.df
    out = (dat.loc[dat['grid_label'] == "gn"][["source_id", "experiment_id", "member_id", "variable_id",
                                                    "zstore", "table_id"]].copy())
    out = out.rename(columns={"source_id": "model", "experiment_id": "experiment",
                                                "member_id": "ensemble", "variable_id": "variable",
                                                "zstore": "zstore", "table_id": "domain"}).copy()
    out = out.drop_duplicates().reset_index(drop=True).copy()

    return out


# Fetch data urls
def get_pangeo_urls(variable, esm, scenario, ensemble):
    """
    Function for pulling urls to access data from
    """
    # Getting full table of pangeo database
    pangeo_table = fetch_pangeo_table()

    # Getting url for given model/variable/scenario/ensemble
    pangeo_urls = pangeo_table[(pangeo_table['model'] == esm) &
                                (pangeo_table['variable'] == variable) &
                                (pangeo_table['domain'] == 'day') &
                                (pangeo_table['experiment'] == scenario) &
                                (pangeo_table['ensemble'] == ensemble)].copy().iloc[0].zstore
    
    return pangeo_urls


def create_tasrange_tasskew_pangeo(output_path, esm, scenario, ensemble):
    # Need to get tas, tasmin, tasmax from pangeo, create tasrange/tasskew, and save it
    # Saving in a created tasrange_tasskew directory in the run directory in intermediate output
    # We need main to check for tasrange/tasskew and pangeo combo, and tell it to instead
    # look at this directory, and use the downloaded method. Then we can wipe that directory
    # after the run

    # Get urls for the required datasets
    tas_urls = get_pangeo_urls('tas', esm, scenario, ensemble)
    tasmax_urls = get_pangeo_urls('tasmax', esm, scenario, ensemble)
    tasmin_urls = get_pangeo_urls('tasmin', esm, scenario, ensemble)

    # Download the data
    tas_data = fetch_nc(tas_urls)
    tasmax_data = fetch_nc(tasmax_urls)
    tasmin_data = fetch_nc(tasmin_urls)

    # Create tasrange
    tasrange_array = tasmax_data['tasmax'] - tasmin_data['tasmin']
    # Create tasskew
    tasskew_array = (tas_data['tas'] - tasmin_data['tasmin']) / tasrange_array

    # Convert to xarray Dataset from DataArray
    tasrange_data = tasrange_array.to_dataset(name='tasrange')
    tasskew_data = tasskew_array.to_dataset(name='tasskew')

    # Create temp directory in intermediate output
    output_path = os.path.join(output_path, 'tasrange_tasskew')
    os.makedirs(os.path.join(output_path), exist_ok=True)

    # First and last day in data as string for file name
    start_str = np.min(pd.DatetimeIndex(tas_data['time'].values)).strftime('%Y%m%d')
    end_str = np.max(pd.DatetimeIndex(tas_data['time'].values)).strftime('%Y%m%d')

    # If tasrange files don't already exist, create them
    try:
        tasrange_files = glob.glob(os.path.join(output_path, f'tasrange_day_{esm}_{scenario}_{ensemble}_*.nc'))
        assert len(tasrange_files) == 0, 'tasrange files already exist'
        tasrange_data.to_netcdf(os.path.join(output_path, f'tasrange_day_{esm}_{scenario}_{ensemble}_{start_str}-{end_str}.nc'), compute=True)
    except AssertionError:
        pass

    # If tasskew files don't already exist, create them
    try:
        tasskew_files = glob.glob(os.path.join(output_path, f'tasskew_day_{esm}_{scenario}_{ensemble}_*.nc'))
        assert len(tasskew_files) == 0, 'tasskew files already exist'
        tasskew_data.to_netcdf(os.path.join(output_path, f'tasskew_day_{esm}_{scenario}_{ensemble}_{start_str}-{end_str}.nc'), compute=True)
    except AssertionError:
        pass
    ...


if __name__ == "__main__":

    # Ignore non-helpful warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings('ignore', category=FutureWarning)

    # Read in run details ================================================================
    # Input path provided from command line
    run_directory = str(sys.argv[1])
    input_path = os.path.join('intermediate', run_directory)

    # Read in .csv
    run_details = pd.read_csv(os.path.join(input_path, 'run_manager_explicit_list.csv'))

    # Get tasks asking for either tasmin or tasmax ======================================
    run_details = run_details[(run_details['Variable'] == 'tasrange') | (run_details['Variable'] == 'tasskew')].copy()

    # Get the available models, scenarios, and ensembles (if available)
    if run_details.iloc[0].stitched:
        create_tasrange_tasskew_stitched(run_details)
    else:
        create_tasrange_tasskew_CMIP(run_details, input_path)
