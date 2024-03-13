#!/bin/bash

#SBATCH --job-name="gr06h00n01t04c064g"
#SBATCH --output=%x.%j.out
#SBATCH --error=%x.%j.err

#SBATCH --time=06:00:00

#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=4

##SBATCH --mem=64G

#SBATCH --partition=memory
#SBATCH --account=research-ceg-he

#SBATCH --mail-user=raul.ortiz@tudelft.nl
#SBATCH --mail-type=ALL

# Load modules:
module load 2023r1
module load openmpi
module load miniconda3

# Set conda env:
#unset CONDA_SHLVL
#source "$(conda info --base)/etc/profile.d/conda.sh"

# Activate conda, run job, deactivate conda
# Make sure to state conda environment name and location accordingly
conda activate /scratch/$USER/conda/hires_env

# Change working directory to location of the files
CUR_DIR=$(pwd)
SOURCE_DIR=/projects/HiResData
EXEC_DIR=/scratch/$USER/HRS/test_raw
#pushd $EXEC_DIR

# set run directory and go there
if [ ! -d "$EXEC_DIR" ]; then
 mkdir -p $EXEC_DIR
fi
cd $EXEC_DIR

# copy files ; modify head arguments as needed
FILES=$(ls $SOURCE_DIR/*map.nc | head -10)
for FILE in $FILES; do
 rsync -vu --progress --no-perms "${FILE}" "${EXEC_DIR}"
 echo "copying ${FILE}"
done

# here's where the I/O test should go
rm RMM_dflowfm_2022_jul21_aug7_sm_0000_map.nc

# > Initialize filenames & script names
filebase=RMM_dflowfm_2022_jul21_aug7_sm # Change to "_map-file" base name (everything before _map.nc)
pythonfile=/home/$USER/hires-processing/src/calc/gradient.py
outfile=$EXEC_DIR/time_written.nc # Change to output location with filename

echo Starting data loading and time-slicing...
TIMENOW=$(date +"The local start_time is %r")
echo $TIMENOW

which python3
#python3 $pythonfile -f ${filebase}_0000_map.nc -o $outfile -bc $SLURM_NTASKS #-d mesh2d_waterdepth mesh2d_hu mesh2d_ucxa mesh2d_ucya mesh2d_Patm mesh2d_windx mesh2d_windy mesh2d_windxu mesh2d_windyu mesh2d_viu mesh2d_turkin1 mesh2d_tureps1 mesh2d_flowelem_ba mesh2d_bldepth mesh2d_flowlink_zu mesh2d_flowlink_zu_bnd # this is for heatflux run data
# profiler run 
python3 -m cProfile -s tottime $pythonfile -f ${filebase}_0001_map.nc -o $outfile -bc $SLURM_NTASKS > ~/%x.%j.pro

echo finished data loading and time-slicing...
TIMENOW=$(date +"The local start_time is %r")
echo $TIMENOW

conda deactivate
