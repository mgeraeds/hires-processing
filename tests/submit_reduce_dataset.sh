#! /bin/bash
#SBATCH --nodes 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=192
#SBATCH --job-name=post
#SBATCH --partition=genoa
#SBATCH --time=1:00:00
#SBATCH --mail-type=BEGIN,END,ERROR

THREAD_COUNT=$SLURM_CPUS_PER_TASK/32
export OMP_NUM_THREADS=$THREAD_COUNT

# > Load modules
module purge
module load 2022
module load Anaconda3/2022.05

CUR_DIR=$(pwd)
EXEC_DIR=/projects/0/einf1300/saltis-wp3-1/C_Work/00_RMM3d_2022_simulations/computations/B02_2022_jul21-aug7/B02_2022_jul21-aug7_sm #/projects/0/einf1300/saltis-wp3-1/C_Work/00_RMM3d_2022_simulations/computations/B03_2022_aug1-2/B03_2022_aug1-2_hourly #/projects/0/einf1300/saltis-wp3-1/C_Work/00_RMM3d_2022_simulations/computations/B03_2022_mrt-may/B03_2022_mrt-may_new/
pushd $EXEC_DIR

# Conda initialization
CONDA_BASE=$(conda info --base)
source $CONDA_BASE/etc/profile.d/conda.sh

# Load conda virtual environment
conda activate dfm_proc_env # > Change this to environment with all of the required packages

# Initialize filenames & script names
filebase=RMM_dflowfm_2022_jul21_aug7_sm
pythonfile=/home/mgeraeds/Repositories/hires-processing/src/reduce/read_write.py
outfile=variable_reduction_test.nc # Change to output location with filename

echo Starting data loading and time-slicing...
TIMENOW=$(date +"The local start_time is %r")
echo $TIMENOW

# Initialize dask-related arguments
max_mem=$((192/SLURM_CPUS_PER_TASK))

which python3
python3 $pythonfile -f ${filebase}_000*_map.nc -o $outfile -m $max_mem -n 8 -k 'mesh2d_sa1' 'mesh2d_node_z' 'mesh2d_ucx' 'mesh2d_ucy' 'mesh2d_ucz' 'mesh2d_vol1' 'mesh2d_vicwwu' 'mesh2d_tem1' 'mesh2d_face_nodes' 'mesh2d_edge_nodes' 'mesh2d' -e '{"mesh2d_face_nodes": {"dtype": "float32"}, "mesh2d_edge_nodes": {"dtype": "float64"}, "mesh2d_nNodes": {"dtype":"int32"}, "mesh2d_nFaces": {"dtype":"int32"}, "mesh2d_nEdges": {"dtype":"int32"}, "mesh2d_flowelem_zcc": {"dtype":"float32"}, "mesh2d_flowelem_zw": {"dtype":"float32"}, "mesh2d_face_x": {"dtype":"float32"}, "mesh2d_face_y": {"dtype":"float32"}, "mesh2d_edge_x": {"dtype":"float32"}, "mesh2d_edge_y": {"dtype":"float32"}, "mesh2d_sa1": {"dtype":"float32"}, "mesh2d_node_x": {"dtype":"float32"}, "mesh2d_node_y": {"dtype":"float32"}, "mesh2d_node_z": {"dtype":"float32"}, "mesh2d_ucx": {"dtype":"float32"}, "mesh2d_ucy": {"dtype":"float32"}, "mesh2d_ucz": {"dtype":"float32"}, "mesh2d_vol1": {"dtype":"float32"}, "mesh2d_vicwwu": {"dtype":"float32"}, "mesh2d_tem1":{"dtype":"float32"}}' > ~/%x.%j.pro