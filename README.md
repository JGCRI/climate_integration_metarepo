# Climate Integration Meta-repo

The purpose of the repo is to be an easy yet comprehensive way to interact with [basd](https://github.com/JGCRI/basd), the Python package that allows you to **bias adjust** and **statistically downscale** arbitrary climate data, <u>without having to write a single line of code</u>, in a way that interacts with other climate tools [STITCHES](https://github.com/JGCRI/stitches) and [Hector](https://github.com/JGCRI/hector).

# Installation

Start by cloning this repository:

```
git clone https://github.com/JGCRI/climate_integration_metarepo.git
```

Set up a Conda virtual environment ([Conda user guide](https://conda.io/projects/conda/en/latest/user-guide/getting-started.html)), and activate it. **Specifically use Conda to manage this environment as we will use a Python package which is only available through Conda.**

Install the Conda dependent package,

```
conda install -c conda-forge xesmf
```

Next, install two Python packages, BASD and STITCHES. You can do this in multiple ways:

### Clone GitHub locally

This is the preferred way, as both BASD and STITCHES are new and quickly developing packages. This allows you to easily pull recent updates, switch branches, and even edit the source if needed. You may even want to default to the `dev` branch. Do this by cloning,

```
git clone https://github.com/JGCRI/basd.git
git clone https://github.com/JGCRI/stitches.git
```

and then installing in your virtual environment using develop mode by navigating to each respective repo and running, 

```
python -m pip install -e .
```


### GitHub Remote
You can also just install from github, and you could still specify the branch you want to use,

```
pip install git+https://github.com/JGCRI/basd.git
pip install git+https://github.com/JGCRI/stitches.git
```

# Usage

To use this repo, we'll create an input file that lists all the models, ensembles, scenarios, climate variables, and time periods we want to adjust. We'll then run `job-script-generation.py` to prepare a script to submit jobs to the slurm scheduler. Finally, we can use that script to run our jobs.

## Editing Input Files

1. Open `run_manager.csv` and edit the available columns:
    * ESM
        * List the names of all the ESMs you want to use
    * ESM_Input_Location
        * Set the path to where the data for the ESM is stored
        * Leave the path empty if you want to use Pangeo to access the ESM data from cloud storage
    * Output_Location
        * Set the path where the output data should be stored
    * Reference Dataset
        * List the names of all the reference dataset you want to use (normally just one)
    * Reference_Input_Location
        * Set the path where the reference dataset is stored
        * This data must be stored locally
    * Variable
        * List the climate variable short names that you want to use
    * Scenario
        * List the scenarios that you want to use
    * Ensemble
        * List the ensembles that you want to use
    * target_period
        * List the year ranges for the reference data that you want to use (normally just use one, the range of the reference data)
        * Use the format start_year-end_year
    * application_period
        * List the year ranges for which you want to adjust
        * You may use the "future period" of the CMIP6 archive for example, 2015-2100. 
        * You generally don't want to use the exact period as the target to avoid over-fitting. For example if the target period is 1970-2014 and we want to adjust a historical period, perhaps use 1950-2014 as the application period.
        * Use the format start_year-end_year
    * daily
        * Whether to save output data at the daily resolution
        * True or False
    * monthly
        * Whether to save output at the monthly resolution
        * True or False
    * stitched
        * Whether input ESM data was created using STITCHES
        * True or False
        * Set to False by default. Using this feature will be explained more later

2. Open `slurm_parameters.csv` and enter details for the slurm scheduler that will be used for your jobs.

3. The file `encoding.csv` describes how the output NetCDF files will be encoded when saved. Mostly the defaults should be good for most applications. You may in particular want to change: 
    * `complevel`, which will change the level of compression applied to your data
    * `time_chunk`, `lat_chunk`, `lon_chunk`, which changes the chunk sizes of each dimension in the output NetCDF. This can effect how other programs interact and read in the data consequently. You can either enter an integer, or "max", which will use the full size of the that dimension in the given data.

4. The file `dask_parameters.csv` changes how [Dask](https://www.dask.org/), the Python package responsible for the parallelization in these processes, will split up (i.e. "chunk") the data. For machines with smaller RAM, you may want to lower from the defaults. The `dask_temp_directory` option gives you a chance to change where Dask stores intermediate files. For example, some computing clusters have a `/scratch/` directory where it is ideal to store temporary files that we don't want to be accidentally stored long term.

5. The file `variable_parameters.csv` may be edited, though the values set in the repo will be good for most cases, and more details are given in the file itself.

You may also create more "run manager" files that can be specific to a given run. Simply duplicate `run_manager.csv` and name it something informative, and edit it accordingly. The file `test_run_manager.csv` is such a file and can be used as an example and uses small data files included in this repo.

## Running

First run `job-script-generation.py` from the root repository level, passing the name of your run manager file as an argument. For example:

```
python code/python/job-script-generation.py test_run_manager.csv
```

You should see two files now in the `intermediate` directory:
1. test_run_manager.job
    * This is the script that will submit your jobs to the slurm scheduler
2. test_run_manager_explicit_list.csv
    * This will list out the details of each run that you requested explicitly.

These will have different names according to the name of your run manager file. It's good to check that these files were generated as you expected. For example that the `.job` file includes all the slurm metadata that you input, and especially the explicit list file is nice to see exactly how many jobs you are requesting, and you can use it to decide to submit jobs one by one, which will follow the order which they are listed in that file.

Then, you're ready to submit your jobs. Do this by running the .job script from the root repository level. For example:
```
sbatch intermediate/test_run_manager.job
```
will submit all of your jobs.

Alternatively, 
```
sbatch --array=0-1 intermediate/test_run_manager.job
```
will submit the first two runs, and the array argument can be set to your liking. This is useful if you don't want to submit all your jobs at once for resource reasons, or if you want to start by testing just one.

### Avoiding the Slurm Scheduler

For the test data for example, which is small, you can choose to avoid using the slurm scheduler. For example run

```
python code/python/main 0 test_run_manager_explicit_list.csv
```
which will run the first job in your list.

## Monitoring Job Progress

When you submit jobs you can view their progress, resource usage, etc. This is because the processes use [Dask](https://www.dask.org/), which give access the Dask dashboard.

There is a hidden directory in this repo `.out`, which stores the files generated by the slurm scheduler. Once your job starts it will print two lines at the top of the file with commands that you can copy to access the Dask dashboard. Which one you will use varies depending on if you are running locally or remote. Generally you will use the remote command, which needs to be slightly edited manually, where you enter your remote details. You can then use your preferred browser to open the specified port.

## Output

Navigate to the output paths that you set in your run manager file. This should be populated with NetCDF files as the run progresses. You can use software like NCO, with the `ncdump` command to view metadata, or you can use software like [Panopoly](https://www.giss.nasa.gov/tools/panoply/) to open and view the data plotted.