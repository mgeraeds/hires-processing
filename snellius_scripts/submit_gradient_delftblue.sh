#!/bin/bash
#SBATCH --job-name="jobname"
#SBATCH --time=01:00:00
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G
#SBATCH --partition=compute
#SBATCH --account=research-ceg-he

# Load modules:
module load 2022r2
module load openmpi
module load miniconda3

# Change working directory lo location of the files
CUR_DIR=$(pwd)
EXEC_DIR=<reference-to-file-directory>
pushd $EXEC_DIR

# Set conda env:
unset CONDA_SHLVL
source "$(conda info --base)/etc/profile.d/conda.sh"

# Activate conda, run job, deactivate conda
conda activate <name-of-my-conda-environment>

# > Initialize filenames & script names
filebase=RMM_dflowfm_2022_B02_jul21-aug7_sm # Change to "_map-file" base name (everything before _map.nc)
pythonfile=../src/calc/gradient.py
outfile= # Change to output location with filename

echo Starting data loading and time-slicing...
TIMENOW=$(date +"The local start_time is %r")
echo $TIMENOW

which python3
python3 $pythonfile -f ${filebase}_0000_map.nc -o $outfile #-d mesh2d_waterdepth mesh2d_hu mesh2d_ucxa mesh2d_ucya mesh2d_Patm mesh2d_windx mesh2d_windy mesh2d_windxu mesh2d_windyu mesh2d_viu mesh2d_turkin1 mesh2d_tureps1 mesh2d_flowelem_ba mesh2d_bldepth mesh2d_flowlink_zu mesh2d_flowlink_zu_bnd # this is for heatflux run data

conda deactivate
