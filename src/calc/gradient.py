import dfm_tools as dfmt
import argparse
import os
import xarray as xr
from dask.distributed import Client
import dask
import numpy as np


if __name__ == '__main__':

    # > Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str, required=True)
    parser.add_argument('-o', '--out_file', type=str)  # type=dir_path)
    parser.add_argument('-bc', '--batch_cores', type=str, required=True)
    args = parser.parse_args()

    # > Get arguments
    file_nc = args.file
    out_file = args.out_file
    batch_cores = args.batch_cores

    n_cores = 4
    n_processes = 4
    n_workers = n_cores * n_processes
    max_mem_alloc = 1.75 * int(batch_cores)  # 1.75 for Snellius,  768/48 = 16 for DelftBlue
    mem_lim = str(int(np.floor(max_mem_alloc))) + 'GB'  # 336 GiB memory for genoa node

    print('Starting client...')
    client = Client(n_workers=16, threads_per_worker=1, memory_limit=mem_lim)
    client.amm.start()  # automatic memory management
    print('Started client.')

    # Open the partitioned dataset with xarray
    print('Loading large dataset...')
    # with ProgressBar():
    ds = dfmt.open_partitioned_dataset(file_nc.replace('_0000_', '_000*_'), chunks={'time': 1, 'mesh2d_nFaces': 400000})

    print('Large dataset loaded.')
    times = ds.time
    print(len(times))
    gridname = ds.ugrid.grid.name

    # > Select only the relevant variables in the new for gradient computation /
    # > salinity variance analysis. {gridname}_node_z is necessary to keep the
    # > nodal dimension.
    ds = ds[[f'{gridname}_node_z', f'{gridname}_sa1', f'{gridname}_u1',
             f'{gridname}_u0', f'{gridname}_ucx', f'{gridname}_ucy', f'{gridname}_ucz', f'{gridname}_tem1',
             f'{gridname}_windx', f'{gridname}_windy', 
             f'{gridname}_vicwwu', f'{gridname}_dtcell']] # f'{gridname}_rho', f'{gridname}_au', f'{gridname}_vol1', f'{gridname}_windstressx', f'{gridname}_windstressy', 

    # Define counter for possible subdivision of times in writing
    i = 0

    for t in range(len(times)):

        # > Select the time
        print(f'Starting selection of timestep {t}...')
        tds = ds.isel(time=[t])
        print(f'Loaded the dataset at timestep {t}.')

        if t == 0:
            # > For t = 0 in the range of timesteps, check if there's a file
            # > already present. If it is, remove it.
            if os.path.isfile(out_file):
                os.remove(out_file)

            # Use dask.delayed to write file to disk
            print('Writing file to disk...')
            print(f"Outputfile: {out_file}")
            write_task = dask.delayed(tds.ugrid.to_netcdf(out_file, mode='w'))

        else:
            # Use dask.delayed to write file to disk
            print('Writing file to disk...')
            print(f"Outputfile: {out_file}")
            write_task = dask.delayed(tds.ugrid.to_netcdf(out_file, mode='a'))

        write_task.compute()
        tds.close()

        # > Update counter
        i += 1

    client.close()