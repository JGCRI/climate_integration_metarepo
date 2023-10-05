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
import glob
import sys

import numpy as np
import pandas as pd
import xarray as xr

import utils


def create_tasmin_tasmax_stitched(run_details):
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


def create_general_CMIP(
                        tas_file_name, tasrange_array, tasskew_file_name, tasmin_file_name, tasmax_file_name,
                        full_out_path, encoding, reset_chunk_sizes, 
                        tasmin_attributes, tasmax_attributes, global_attributes
                    ):
    # Open data
    tas_data = xr.open_mfdataset(os.path.join(full_out_path, tas_file_name))
    tasrange_data = xr.open_mfdataset(os.path.join(full_out_path, tasrange_file_name))
    tasskew_data = xr.open_mfdataset(os.path.join(full_out_path, tasskew_file_name))

    # Create tasmin
    tasmin_array = tas_data['tas'] - (tasskew_data['tasskew'] * tasrange_data['tasrange'])

    # Create tasmax
    tasmax_array = tasmin_array + tasrange_data['tasrange']

    # Convert to xarray DataSet from DataArray
    tasmin_data = tasmin_array.to_dataset(name='tasmin')
    tasmax_data = tasmax_array.to_dataset(name='tasmax')

    # Set global attributes
    tasmin_data.attrs = global_attributes
    tasmax_data.attrs = global_attributes

    # Set variable attributes
    tasmin_data['tasmin'].attrs = tasmin_attributes
    tasmax_data['tasmax'].attrs = tasmax_attributes

    # Reset Chunk sizes
    if reset_chunksizes:
        encoding['chunksizes'] = utils.reset_chunk_sizes(encoding['chunksizes'], tas_data.dims)

    # Save data
    tasmin_data.to_netcdf(os.path.join(full_out_path, tasmin_file_name), encoding={'tasmin': encoding} compute=True)
    tasmax_data.to_netcdf(os.path.join(full_out_path, tasmax_file_name), encoding={'tasmax': encoding} compute=True)

    ...


def create_monthly_ba_CMIP(
                            esm, scenario, ensemble, start, end, ref_name, 
                            output_location, encoding, reset_chunk_sizes,
                            tasmin_attributes, tasmax_attributes,
                            global_monthly_attributes
                        ):
    # File names
    tas_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tas_global_monthly_{start}_{end}.nc'
    tasrange_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasrange_global_monthly_{start}_{end}.nc'
    tasskew_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasskew_global_monthly_{start}_{end}.nc'
    tasmin_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasmin_global_monthly_{start}_{end}.nc'
    tasmax_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasmax_global_monthly_{start}_{end}.nc'

    # Full output_location
    full_out_path = os.path.join(output_location, ref_name, esm, scenario, 'ba')

    # Save data using generic saver function
    create_general_CMIP(
        tas_file_name, tasrange_file_name, tasskew_file_name, tasmin_file_name, tasmax_file_name,
        full_out_path, encoding, reset_chunk_sizes, 
        tasmin_attributes, tasmax_attributes, global_monthly_attributes
    )

    ...


def create_daily_ba_CMIP(
                            esm, scenario, ensemble, start, end, ref_name, 
                            output_location, encoding, reset_chunk_sizes,
                            tasmin_attributes, tasmax_attributes,
                            global_daily_attributes
                        ):
    # File names
    tas_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tas_global_daily_{start}_{end}.nc'
    tasrange_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasrange_global_daily_{start}_{end}.nc'
    tasskew_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasskew_global_daily_{start}_{end}.nc'
    tasmin_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasmin_global_daily_{start}_{end}.nc'
    tasmax_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasmax_global_daily_{start}_{end}.nc'

    # Full output_location
    full_out_path = os.path.join(output_location, ref_name, esm, scenario, 'ba')

    # Save data using generic saver function
    create_general_CMIP(
        tas_file_name, tasrange_file_name, tasskew_file_name, tasmin_file_name, tasmax_file_name,
        full_out_path, encoding, reset_chunk_sizes, 
        tasmin_attributes, tasmax_attributes, global_daily_attributes
    )

    ...


def create_monthly_basd_CMIP(
                            esm, scenario, ensemble, start, end, ref_name, 
                            output_location, encoding, reset_chunk_sizes,
                            tasmin_attributes, tasmax_attributes,
                            global_monthly_attributes
                        ):
    # File names
    tas_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tas_global_monthly_{start}_{end}.nc'
    tasrange_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasrange_global_monthly_{start}_{end}.nc'
    tasskew_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasskew_global_monthly_{start}_{end}.nc'
    tasmin_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasmin_global_monthly_{start}_{end}.nc'
    tasmax_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasmax_global_monthly_{start}_{end}.nc'

    # Full output_location
    full_out_path = os.path.join(output_location, ref_name, esm, scenario, 'basd')

    # Save data using generic saver function
    create_general_CMIP(
        tas_file_name, tasrange_file_name, tasskew_file_name, tasmin_file_name, tasmax_file_name,
        full_out_path, encoding, reset_chunk_sizes, 
        tasmin_attributes, tasmax_attributes, global_monthly_attributes
    )

    ...


def create_daily_basd_CMIP(
                            esm, scenario, ensemble, start, end, ref_name, 
                            output_location, encoding, reset_chunk_sizes,
                            tasmin_attributes, tasmax_attributes,
                            global_daily_attributes
                        ):
    # File names
    tas_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tas_global_daily_{start}_{end}.nc'
    tasrange_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasrange_global_daily_{start}_{end}.nc'
    tasskew_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasskew_global_daily_{start}_{end}.nc'
    tasmin_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasmin_global_daily_{start}_{end}.nc'
    tasmax_file_name = f'{esm}_{ensemble}_{ref_name}_{scenario}_tasmax_global_daily_{start}_{end}.nc'

    # Full output_location
    full_out_path = os.path.join(output_location, ref_name, esm, scenario, 'basd')

    # Save data using generic saver function
    create_general_CMIP(
        tas_file_name, tasrange_file_name, tasskew_file_name, tasmin_file_name, tasmax_file_name,
        full_out_path, encoding, reset_chunk_sizes, 
        tasmin_attributes, tasmax_attributes, global_daily_attributes
    )

    ...


def create_tasmin_tasmax_CMIP(
                                run_details, encoding, reset_chunk_sizes, 
                                tasmin_attributes, tasmax_attributes,
                                global_monthly_attributes, global_daily_attributes
                            ):
    # List of all models, scenarios and ensemble members being used
    scenarios = np.append( np.unique(run_details.Scenario.values), ['historical'] )
    esms = np.unique(run_details.ESM.values)
    ensembles = np.unique(run_details.Ensemble.values)
    ref_datasets = np.unique(run_details.Reference_Dataset.values)
    application_periods = np.unique(run_details.application_period.values)

    for esm in esms:
        for scenario in scenarios:
            for ensemble in ensembles:
                for ref_name in ref_datasets:
                    for application_period in application_periods:
                        
                        print(f'Creating tasrange and tasskew for {esm} {scenario}')
                        # Get input/output location
                        current_task = run_details[
                            (run_details['ESM'] == esm) &
                            (run_details['Scenario'] == scenario) &
                            (run_details['Ensemble'] == ensemble) &
                            (run_details['Reference_Dataset'] == ensemble) &
                            (run_details['application_period'] == ensemble) 
                        ]
                        output_location = current_task['Output_Location'].values[0]
                        using_pangeo = pd.isna(output_location)

                        # Start and End years
                        start, end = str.split(application_period, '-')

                        # Try to create daily bias adjusted tasmin and tasmax
                        try:
                            create_daily_ba_CMIP(
                                esm, scenario, ensemble, start, end, ref_name, 
                                output_location, encoding, reset_chunk_sizes,
                                tasmin_attributes, tasmax_attributes,
                                global_daily_attributes
                            )
                        except:
                            print(f'Waringing, could not create daily bias adjusted tasmin and tasmax')
                            pass

                        # Try to create monthly bias adjusted tasmin and tasmax
                        try:
                            create_monthly_ba_CMIP(
                                esm, scenario, ensemble, start, end, ref_name, 
                                output_location, encoding, reset_chunk_sizes,
                                tasmin_attributes, tasmax_attributes,
                                global_monthly_attributes
                            )
                        except:
                            print(f'Waringing, could not create monthly bias adjusted tasmin and tasmax')
                            pass

                        # Try to create daily bias adjusted and downscaled tasmin and tasmax
                        try:
                            create_daily_basd_CMIP(
                                esm, scenario, ensemble, start, end, ref_name, 
                                output_location, encoding, reset_chunk_sizes,
                                tasmin_attributes, tasmax_attributes,
                                global_daily_attributes
                            )
                        except:
                            print(f'Waringing, could not create daily bias adjusted and downscaled tasmin and tasmax')
                            pass

                        # Try to create monthly bias adjusted tasmin and tasmax
                        try:
                            create_monthly_basd_CMIP(
                                esm, scenario, ensemble, start, end, ref_name, 
                                output_location, encoding, reset_chunk_sizes,
                                tasmin_attributes, tasmax_attributes,
                                global_monthly_attributes
                            )
                        except:
                            print(f'Waringing, could not create monthly bias adjusted and downscaled tasmin and tasmax')
                            pass

                        ...
                    ...
                ...
            ...
        ...
    ...


if __name__ == "__main__":

    # Read in run details ================================================================
    # Input path provided from command line
    run_directory = str(sys.argv[1])
    input_path = os.path.join('intermediate', run_directory)

    # Read in .csv
    run_details = pd.read_csv(os.path.join(input_path, 'run_manager_explicit_list.csv'))

    # Get tasks asking for either tasmin or tasmax ======================================
    run_details = run_details[(run_details['Variable'] == 'tasrange') | (run_details['Variable'] == 'tasskew')].copy()

    # Read encoding settings
    encoding, reset_chunksizes = utils.get_encoding(os.path.join('input', run_name))

    # Get attributes
    tasmin_attributes, global_monthly_attributes, global_daily_attributes = utils.get_attributes('tasmin', os.path.join('input', run_name))
    tasmax_attributes, _, _ = utils.get_attributes('tasmax', os.path.join('input', run_name))

    # Get the available models, scenarios, and ensembles (if available)
    if run_details.iloc[0].stitched:
        create_tasmin_tasmax_stitched(run_details)
    else:
        create_tasmin_tasmax_CMIP(
            run_details, encoding, reset_chunk_sizes, 
            tasmin_attributes, tasmax_attributes,
            global_monthly_attributes, global_daily_attributes
        )
