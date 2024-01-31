#! /bin/bash
#SBATCH --nodes 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=192
#SBATCH --job-name=post
#SBATCH --partition=genoa
#SBATCH --time=1:00:00
#SBATCH --mail-type=BEGIN,END,ERROR

#THREAD_COUNT=$SLURM_CPUS_PER_TASK/32
#export OMP_NUM_THREADS=$THREAD_COUNT

# > Load modules
module purge
module load 2022
module load Anaconda3/2022.05

CUR_DIR=$(pwd)
EXEC_DIR=/projects/0/einf1300/saltis-wp3-1/C_Work/00_RMM3d_2022_simulations/computations/B03_2022_aug1-2/B03_2022_aug1-2_hourly #/projects/0/einf1300/saltis-wp3-1/C_Work/00_RMM3d_2022_simulations/computations/B03_2022_mrt-may/B03_2022_mrt-may_new/
pushd $EXEC_DIR

# > Conda initialization
CONDA_BASE=$(conda info --base)
source $CONDA_BASE/etc/profile.d/conda.sh
# > Load conda virtual environment
conda activate dfm_proc_env # > Change this to environment with all of the required packages

# > Initialize filenames & script names
filebase=RMM_dflowfm_2022_B03_aug1_2 # Change to "_map-file" base name
pythonfile=../src/calc/gradient.py
outfile= # Change to output location with filename

echo Starting data loading and time-slicing...
TIMENOW=$(date +"The local start_time is %r")
echo $TIMENOW

which python3
python3 $pythonfile -f ${filebase}_0000_map.nc -o $outfile -bc $SLURM_NTASKS #-d mesh2d_waterdepth mesh2d_hu mesh2d_ucxa mesh2d_ucya mesh2d_Patm mesh2d_windx mesh2d_windy mesh2d_windxu mesh2d_windyu mesh2d_viu mesh2d_turkin1 mesh2d_tureps1 mesh2d_flowelem_ba mesh2d_bldepth mesh2d_flowlink_zu mesh2d_flowlink_zu_bnd # this is for heatflux run data
