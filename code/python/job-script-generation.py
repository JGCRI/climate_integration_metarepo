"""
Description: This script will look at the input run manager file and create a bash script to submit all jobs 
             in parallel using the slurm manager, and a csv file that explicitly lists all combinations of 
             runs requested.
Author: Noah Prime
Modified: August 7, 2023
Input:
    - input/<run manager>.csv - file that specifies all the runs requested
    - input/slurm_parameters.csv - parameters to be used for slurm scheduler
Output:
    - intermediate/<run_manager>_explicit_list.csv - file that explicitly lists out the details of each run requested
    - intermediate/<run_manager>.job - bash file for submitting jobs to slurm scheduler
"""

# Import Libraries
import os
import sys

import numpy as np
import pandas as pd

if __name__ == "__main__":

    # Read in desired run
    run_name = str(sys.argv[1])

    # Define paths
    input_files_path = 'input'
    intermediate_path = 'intermediate'
    run_manager_file = 'run_manager.csv'

    # Helper function to remove nans
    def remove_nas(x):
        return x[~pd.isnull(x)]

    # Read in user defined job requests
    run_manager_df = pd.read_csv(os.path.join(input_files_path, run_name, run_manager_file))
    esms = remove_nas(run_manager_df['ESM'].values)
    esm_input_paths = remove_nas(run_manager_df['ESM_Input_Location'].values)
    output_paths = remove_nas(run_manager_df['Output_Location'].values)
    ref_datasets = remove_nas(run_manager_df['Reference_Dataset'].values)
    ref_datasets_paths = remove_nas(run_manager_df['Reference_Input_Location'].values)
    variables = remove_nas(run_manager_df['Variable'].values)
    scenarios = remove_nas(run_manager_df['Scenario'].values)
    ensembles = remove_nas(run_manager_df['Ensemble'].values)
    target_periods = remove_nas(run_manager_df['target_period'].values)
    application_periods = remove_nas(run_manager_df['application_period'].values)
    daily = remove_nas(run_manager_df['daily'].values)
    monthly = remove_nas(run_manager_df['monthly'].values)
    stitched = remove_nas(run_manager_df['stitched'].values)

    # If you want to use BASD for tasmin or tasmax, need to use tas, tasrange and tasskew to do so indirectly
    # So here we make sure to have those variables present when tasmax and/or tasmin is present,
    # and remove the direct tasmin/tasmax calls. We will create these later from the results
    if ('tasmax' in variables) or ('tasmin' in variables):
        variables = np.union1d(np.setdiff1d(variables, ['tasmax', 'tasmin']), ['tas', 'tasrange', 'tasskew'])

    # If no ensembles given (this happens when we're using STITCHED data)
    if len(ensembles) == 0:
        # Get all combinations (as an array, each row represents a single job)
        mesh_array = np.array(np.meshgrid(esms, 
                                        variables, 
                                        scenarios, 
                                        ref_datasets, 
                                        target_periods, 
                                        application_periods)).T.reshape(-1,6)
        # Convert to pandas DataFrame and add back in columns that didn't need extra enumeration
        mesh_df = pd.DataFrame(mesh_array, columns = ['ESM', 'Variable', 'Scenario', 'Reference_Dataset',
                                                'target_period', 'application_period'])
    # When using ensemble members (using standard CMIP data)
    else:
        # Get all combinations (as an array, each row represents a single job)
        mesh_array = np.array(np.meshgrid(esms, 
                                    variables, 
                                    scenarios, 
                                    ensembles,
                                    ref_datasets, 
                                    target_periods, 
                                    application_periods)).T.reshape(-1,7)
        # Convert to pandas DataFrame and add back in columns that didn't need extra enumeration
        mesh_df = pd.DataFrame(mesh_array, columns = ['ESM', 'Variable', 'Scenario', 'Ensemble', 'Reference_Dataset',
                                                'target_period', 'application_period'])


    # Merge in esm input locations
    mesh_df = mesh_df.merge(run_manager_df[['ESM', 'ESM_Input_Location']], on='ESM', how='inner')
    # Merge in reference dataset input locations
    mesh_df = mesh_df.merge(run_manager_df[['Reference_Dataset', 'Reference_Input_Location']], on='Reference_Dataset', how='inner')
    # Merge in output paths
    mesh_df = mesh_df.merge(run_manager_df[['ESM', 'Output_Location']], on='ESM', how='inner')
    # Add daily and monthly bools
    mesh_df['daily'] = daily[0]
    mesh_df['monthly'] = monthly[0]
    mesh_df['stitched'] = stitched[0]


    # Make new directory if not already created
    os.makedirs(os.path.join(intermediate_path, run_name), exist_ok=True)

    # Save dataframe of every run to csv
    mesh_df.to_csv(os.path.join(intermediate_path, run_name, f'run_manager_explicit_list.csv'), index=False)

    # Read in parameters relating to slurm
    slurm_params = pd.read_csv(os.path.join(input_files_path, run_name, 'slurm_parameters.csv'))
    account = slurm_params[slurm_params['parameter'] == 'account']['value'].values[0]
    time = slurm_params[slurm_params['parameter'] == 'time']['value'].values[0]
    partition = slurm_params[slurm_params['parameter'] == 'partition']['value'].values[0]
    max_concurrent = slurm_params[slurm_params['parameter'] == 'max_concurrent']['value'].values[0]
    email = slurm_params[slurm_params['parameter'] == 'email']['value'].values[0]
    mail_type = slurm_params[slurm_params['parameter'] == 'mail-type']['value'].values[0]
    conda_env = slurm_params[slurm_params['parameter'] == 'conda_env']['value'].values[0]

    # Create bash file for submitting all BASD jobs to slurm
    with open(os.path.join(intermediate_path, run_name, 'basd.job'), 'w') as job_file:
        job_file.writelines(f"#!/bin/bash\n\n\n")
        job_file.writelines('# Slurm Settings\n')
        job_file.writelines(f"#SBATCH --account={account}\n")
        job_file.writelines(f"#SBATCH --partition={partition}\n")
        job_file.writelines(f"#SBATCH --job-name={run_name}_BASD.job\n")
        job_file.writelines(f"#SBATCH --time={time}\n")
        job_file.writelines(f"#SBATCH --mail-type={mail_type}\n")
        job_file.writelines(f"#SBATCH --mail-user={email}\n")
        job_file.writelines(f"#SBATCH --output=.out/{run_name}_BASD_%A_%a.out\n")
        job_file.writelines(f"#SBATCH --array=0-{mesh_df.shape[0]-1}%{max_concurrent}\n\n\n")
        job_file.writelines('# Load Modules\n')
        job_file.writelines('module load gcc/11.2.0\n')
        job_file.writelines('module load python/miniconda3.9\n')
        job_file.writelines('source /share/apps/python/miniconda3.9/etc/profile.d/conda.sh\n\n')
        job_file.writelines('# activate conda environment\n')
        job_file.writelines(f'conda activate {conda_env}\n\n')
        job_file.writelines('# Timing\n')
        job_file.writelines('start=`date +%s.%N`\n\n')
        job_file.writelines('# Run script\n')
        job_file.writelines(f"python code/python/main.py $SLURM_ARRAY_TASK_ID {run_name}\n\n")
        job_file.writelines('# End timing and print runtime\n')
        job_file.writelines('end=`date +%s.$N`\n')
        job_file.writelines('runtime=$( echo "($end - $start) / 60" | bc -l )\n')
        job_file.writelines('echo "Run completed in $runtime minutes"\n')

    # Create bash file for generating tasrange and tasskew files
    with open(os.path.join(intermediate_path, run_name, 'tasrange_tasskew.job'), 'w') as job_file:
        job_file.writelines(f"#!/bin/bash\n\n\n")
        job_file.writelines('# Slurm Settings\n')
        job_file.writelines(f"#SBATCH --account={account}\n")
        job_file.writelines(f"#SBATCH --partition={partition}\n")
        job_file.writelines(f"#SBATCH --job-name={run_name}_tasrange_tasskew.job\n")
        job_file.writelines(f"#SBATCH --time={time}\n")
        job_file.writelines(f"#SBATCH --mail-type={mail_type}\n")
        job_file.writelines(f"#SBATCH --mail-user={email}\n")
        job_file.writelines(f"#SBATCH --output=.out/{run_name}_tasrange_tasskew.out\n\n\n")
        job_file.writelines('# Load Modules\n')
        job_file.writelines('module load gcc/11.2.0\n')
        job_file.writelines('module load python/miniconda3.9\n')
        job_file.writelines('source /share/apps/python/miniconda3.9/etc/profile.d/conda.sh\n\n')
        job_file.writelines('# activate conda environment\n')
        job_file.writelines(f'conda activate {conda_env}\n\n')
        job_file.writelines('# Timing\n')
        job_file.writelines('start=`date +%s.%N`\n\n')
        job_file.writelines('# Run script\n')
        job_file.writelines(f"python code/python/create_tasrange_tasskew.py {run_name}\n\n")
        job_file.writelines('# End timing and print runtime\n')
        job_file.writelines('end=`date +%s.$N`\n')
        job_file.writelines('runtime=$( echo "($end - $start) / 60" | bc -l )\n')
        job_file.writelines('echo "Run completed in $runtime minutes"\n')

    # Create bash file for generating tasmin and tasmax files
    with open(os.path.join(intermediate_path, run_name, 'tasmin_tasmax.job'), 'w') as job_file:
        job_file.writelines(f"#!/bin/bash\n\n\n")
        job_file.writelines('# Slurm Settings\n')
        job_file.writelines(f"#SBATCH --account={account}\n")
        job_file.writelines(f"#SBATCH --partition={partition}\n")
        job_file.writelines(f"#SBATCH --job-name={run_name}_tasmin_tasmax.job\n")
        job_file.writelines(f"#SBATCH --time={time}\n")
        job_file.writelines(f"#SBATCH --mail-type={mail_type}\n")
        job_file.writelines(f"#SBATCH --mail-user={email}\n")
        job_file.writelines(f"#SBATCH --output=.out/{run_name}_tasmin_tasmax.out\n\n\n")
        job_file.writelines('# Load Modules\n')
        job_file.writelines('module load gcc/11.2.0\n')
        job_file.writelines('module load python/miniconda3.9\n')
        job_file.writelines('source /share/apps/python/miniconda3.9/etc/profile.d/conda.sh\n\n')
        job_file.writelines('# activate conda environment\n')
        job_file.writelines(f'conda activate {conda_env}\n\n')
        job_file.writelines('# Timing\n')
        job_file.writelines('start=`date +%s.%N`\n\n')
        job_file.writelines('# Run script\n')
        job_file.writelines(f"python code/python/create_tasmin_tasmax.py {run_name}\n\n")
        job_file.writelines('# End timing and print runtime\n')
        job_file.writelines('end=`date +%s.$N`\n')
        job_file.writelines('runtime=$( echo "($end - $start) / 60" | bc -l )\n')
        job_file.writelines('echo "Run completed in $runtime minutes"\n')

    # Create bash file for managing the above files in order
    with open(os.path.join(intermediate_path, run_name, 'manager.job'), 'w') as job_file:
        job_file.writelines(f"#!/bin/bash\n\n\n")

        job_file.writelines('# Slurm Settings\n')
        job_file.writelines(f"#SBATCH --account={account}\n")
        job_file.writelines(f"#SBATCH --partition={partition}\n")
        job_file.writelines(f"#SBATCH --job-name={run_name}_manager.job\n")
        job_file.writelines(f"#SBATCH --time={time}\n")
        job_file.writelines(f"#SBATCH --mail-type={mail_type}\n")
        job_file.writelines(f"#SBATCH --mail-user={email}\n")
        job_file.writelines(f"#SBATCH --output=.out/{run_name}_manager.out\n\n\n")

        job_file.writelines('# Load Modules\n')
        job_file.writelines('module load gcc/11.2.0\n')
        job_file.writelines('module load python/miniconda3.9\n')
        job_file.writelines('source /share/apps/python/miniconda3.9/etc/profile.d/conda.sh\n\n')

        job_file.writelines('# activate conda environment\n')
        job_file.writelines(f'conda activate {conda_env}\n\n')

        job_file.writelines('# Timing\n')
        job_file.writelines('start=`date +%s.%N`\n\n')

        if stitched:
            job_file.writelines('# Run STITCHED data generation script\n')
            job_file.writelines(f"stitch_id=$(sbatch --parsable intermediate/{run_name}/stitch.job)\n\n")

            job_file.writelines('# Run tasrange and tasskew creation job\n')
            job_file.writelines(f"range_skew_id=$(sbatch --parsable --dependency=afterok:$stitch_id intermediate/{run_name}/tasrange_tasskew.job)\n\n")            
        else:
            job_file.writelines('# Run tasrange and tasskew creation job\n')
            job_file.writelines(f"range_skew_id=$(sbatch --parsable intermediate/{run_name}/tasrange_tasskew.job)\n\n")

        job_file.writelines('# Run bias adjustment and downscaling\n')
        job_file.writelines(f"basd_id=$(sbatch --parsable --dependency=afterok:$range_skew_id intermediate/{run_name}/basd.job)\n\n")

        job_file.writelines('# Run tasmin and tasmax creation job\n')
        job_file.writelines(f"min_max_id=$(sbatch --parsable --dependency=afterok:$basd_id intermediate/{run_name}/tasmin_tasmax.job)\n\n")

        job_file.writelines('# End timing and print runtime\n')
        job_file.writelines('end=`date +%s.$N`\n')
        job_file.writelines('runtime=$( echo "($end - $start) / 60" | bc -l )\n')
        job_file.writelines('echo "Run completed in $runtime minutes"\n')

    # Create bash file for generating STITCHED data
    if stitched:
        with open(os.path.join(intermediate_path, run_name, 'stitch.job'), 'w') as job_file:
            job_file.writelines(f"#!/bin/bash\n\n\n")
            job_file.writelines('# Slurm Settings\n')
            job_file.writelines(f"#SBATCH --account={account}\n")
            job_file.writelines(f"#SBATCH --partition={partition}\n")
            job_file.writelines(f"#SBATCH --job-name={run_name}_stitch.job\n")
            job_file.writelines(f"#SBATCH --time={time}\n")
            job_file.writelines(f"#SBATCH --mail-type={mail_type}\n")
            job_file.writelines(f"#SBATCH --mail-user={email}\n")
            job_file.writelines(f"#SBATCH --output=.out/{run_name}_stitch.out\n\n\n")
            job_file.writelines('# Load Modules\n')
            job_file.writelines('module load gcc/11.2.0\n')
            job_file.writelines('module load python/miniconda3.9\n')
            job_file.writelines('source /share/apps/python/miniconda3.9/etc/profile.d/conda.sh\n\n')
            job_file.writelines('# activate conda environment\n')
            job_file.writelines(f'conda activate {conda_env}\n\n')
            job_file.writelines('# Timing\n')
            job_file.writelines('start=`date +%s.%N`\n\n')
            job_file.writelines('# Run script\n')
            job_file.writelines(f"python code/python/generate_stitched_data.py {run_name}\n\n")
            job_file.writelines('# End timing and print runtime\n')
            job_file.writelines('end=`date +%s.$N`\n')
            job_file.writelines('runtime=$( echo "($end - $start) / 60" | bc -l )\n')
            job_file.writelines('echo "Run completed in $runtime minutes"\n')