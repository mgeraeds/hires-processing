#!/bin/bash
#SBATCH --job-name="test-writing"
#SBATCH --time=01:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --partition=memory
#SBATCH --account=research-ceg-he

# Load modules:
module load 2022r2
module load openmpi
module load miniconda3

# Change working directory to location of the files
CUR_DIR=$(pwd)
EXEC_DIR=/scratch/meggeraeds/test_raw
pushd $EXEC_DIR

# Set conda env:
unset CONDA_SHLVL
source "$(conda info --base)/etc/profile.d/conda.sh"

# Activate conda, run job, deactivate conda
conda activate hires_env

# > Initialize filenames & script names
filebase=RMM_dflowfm_2022_B02_jul21-aug7_sm # Change to "_map-file" base name (everything before _map.nc)
pythonfile=/home/meggeraeds/hires-processing/src/calc/gradient.py
outfile=/scratch/meggeraeds/test_out/time_written.nc # Change to output location with filename

echo Starting data loading and time-slicing...
TIMENOW=$(date +"The local start_time is %r")
echo $TIMENOW

which python3
python3 $pythonfile -f ${filebase}_0000_map.nc -o $outfile -bc $SLURM_NTASKS #-d mesh2d_waterdepth mesh2d_hu mesh2d_ucxa mesh2d_ucya mesh2d_Patm mesh2d_windx mesh2d_windy mesh2d_windxu mesh2d_windyu mesh2d_viu mesh2d_turkin1 mesh2d_tureps1 mesh2d_flowelem_ba mesh2d_bldepth mesh2d_flowlink_zu mesh2d_flowlink_zu_bnd # this is for heatflux run data

conda deactivate
