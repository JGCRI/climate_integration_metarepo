"""
Script for generated STITCHED datasets
Lots to update here, rough outline
"""

# Import Packages ----------------------------------------
import os
import sys

import pangeo
import xarray as xr

import pkg_resources
import pandas as pd
import numpy as np
import stitches
import stitches.fx_processing as fxp

# Define Functions ----------------------------------------

def get_archive():
    """
    Function to get the data archive
    TODO: Add option to specify end_yr_vector somehow
    """
    # Download data if not already present
    if not os.path.isfile(pkg_resources.resource_filename('stitches', 'data/matching_archive_staggered.csv')):
        stitches.install_pkgdata.install_package_data()

    # read in the package data of all ESMs-Scenarios-ensemble members avail.
    path = pkg_resources.resource_filename('stitches', 'data/matching_archive_staggered.csv')
    data = pd.read_csv(path)

    # Subset the data to use chunks starting at 2100 and going back in 9 year intervals
    end_yr_vector = np.arange(2100,1800,-9)
    data = stitches.fx_processing.subset_archive(staggered_archive = data, end_yr_vector = end_yr_vector)

    # Return
    return data

def interp(years, values):
    min_year = min(years); max_year = max(years)
    new_years = np.arange(min_year, max_year+1)
    new_values = np.zeros(len(new_years))

    for index, year in enumerate(new_years):
        if np.isin(year, years):
            new_values[index] = values[year == years][0]
        else:
            less_year = max(years[year > years])
            more_year = min(years[year < years])
            less_value = values[np.where(less_year == years)[0][0]]
            more_value = values[np.where(more_year == years)[0][0]]
            p = (year - less_year)/(more_year - less_year)
            new_values[index] = p * more_value + (1-p) * less_value

    return new_years, new_values

def format_data_for_stitches(interped_data, model, ensemble, experiment):
    # Variable, model, ensemble, experiment columns
    # TODO: Edit how these can be input
    interped_data['variable'] = 'tas'
    interped_data['model'] = model
    interped_data['ensemble'] = ensemble
    interped_data['experiment'] = experiment

    # Convert to tas anomaly
    interped_data.value = interped_data.value - np.mean(interped_data.value[(interped_data.year <= 2014) & (interped_data.year >= 1995)])

    # Sort columns
    formatted_traj = interped_data[['variable', 'experiment', 'ensemble', 'model', 'year', 'value']]
    
    # Return
    return formatted_traj

def get_recipe(target_data, archive_data, variables):
    # Get recipe
    stitches_recipe = stitches.make_recipe(target_data, archive_data, tol=0., N_matches=1, res='day', non_tas_variables=[var for var in variables if var != 'tas'])
    # Make sure last period has same length in archive and target
    last_period_length = stitches_recipe['target_end_yr'].values[-1] - stitches_recipe['target_start_yr'].values[-1]
    asy = stitches_recipe['archive_start_yr'].values
    asy[-1] = stitches_recipe['archive_end_yr'].values[-1] - last_period_length
    stitches_recipe['archive_start_yr'] = asy.copy()
    
    return stitches_recipe

def generate_stitched(esm, variables, time_series, years, ensemble, experiment, trajectory_model, output_path, chunk_sizes = 9):
    # Get full archive data
    data = get_archive()
    # Get archive data for specific model
    model_data = data[(data["model"] == esm) &
                    (data["experiment"].str.contains('ssp'))]

    # Interpolate data
    years, temps = interp(years, time_series)
    interped_data = pd.DataFrame({'year': years, 'value': temps})

    # Format data into STICHES format
    formatted_data = format_data_for_stitches(interped_data, trajectory_model, ensemble, experiment)

    # Chunk data
    target_chunk = fxp.chunk_ts(formatted_data, n=chunk_sizes)
    target_data = fxp.get_chunk_info(target_chunk)

    # Make Recipe
    stitches_recipe = get_recipe(target_data, model_data, variables)

    # Make gridded datasets
    outputs = stitches.gridded_stitching(output_path, stitches_recipe)

    return outputs


if __name__ == "__main__":

# Define Constants ----------------------------------------
    # TODO: This stuff needs to be read in from user input

    # Where to save data
    out_path = str(sys.argv[1])

    # This will somehow need to be passed in in a standardized way in the future
    data_path = str(sys.argv[2])

    # Model Choice
    esm = 'MRI-ESM2-0'
    # esm = 'MIROC6'

    # Experiment Name
    experiment = 'GCAM_ref'

    # Ensemble Name
    ensemble = 'hectorUI1'

    # Source Name
    trajectory_source = 'Hector'

    # Variables Output
    variables = ['tas', 'pr', 'hurs', 'sfcWind', 'tasmin', 'tasmax', 'rlds', 'rsds']

    # Getting our trajectory to use (in this case GCAM reference scenario)
    file_name = 'gcam6_ref_default.csv'
    time_series_df = pd.read_csv(os.path.join(data_path, file_name))

    # Getting the time series and years as numpy arrays
    tas_time_series = np.array(time_series_df.temp.values)
    years = np.array( time_series_df.year.values ).astype(int)

    # Run STITCHING process
    generate_stitched(esm, variables, tas_time_series, years, ensemble, experiment, trajectory_source, out_path)
