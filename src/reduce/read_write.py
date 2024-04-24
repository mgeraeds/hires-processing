import dfm_tools as dfmt
import argparse
import os
from dask.distributed import Client
import dask
import xarray as xr
import glob
import numpy as np
import warnings


if __name__ == '__main__':

    # > Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str, required=True)
    parser.add_argument('-o', '--out_file', type=str)  # type=dir_path)
    parser.add_argument('-bc', '--batch_cores', type=str, required=True)
    parser.add_argument('-kv', '--keep_variables', nargs='+', type=str, required=True)
    args = parser.parse_args()

    # > Get arguments
    file_nc = args.file
    out_file = args.out_file
    batch_cores = args.batch_cores
    keep_variables = args.keep_variables

    ## 1. Set up Dask client
    #----------------------------------------------------------------------------------------
    n_cores = 4
    n_processes = 4
    n_workers = n_cores * n_processes
    max_mem_alloc = 1.75 * int(batch_cores)  # 1.75 = 768/48 = 16
    mem_lim = str(int(np.floor(max_mem_alloc))) + 'GB'  # 336 GiB memory for genoa node

    print(f"Allocated memory to the dask client is {mem_lim}.")

    print('Starting client...')
    client = Client(n_workers=n_processes, memory_limit=mem_lim)
    client.amm.start()  # automatic memory management
    print('Started client.')

    ## 2. Input tests
    #----------------------------------------------------------------------------------------
    # 2.1. Loop through all of the files and write when there is something wrong with the file
    file_list = glob.glob(file_nc)

    # 2.2 Make a list for corrupted files
    corrupted = []

    for file in file_list:

        file_name = file.split('/')[-1]
        
        try:
            test = xr.open_dataset(file)
        except:
            warnings.warn(f'The file {file_name} seems to be corrupted. Check your inputs.', UserWarning)
            corrupted.append(file)

    # 2.3 Check if there are any corrupted files in the listed files
    if len(corrupted) > 0:
        print('There are corrupted files. These will be taken out of the file list for the next tests.')
        
    # 2.4 Get the entire input list and remove the corrupted files
    input_files = [f for f in glob.glob(file_nc) if not f in corrupted]

    ## 3. Open the partitioned dataset with xarray
    #----------------------------------------------------------------------------------------
    print('Loading large dataset with time dimension chunked...')
    ds = dfmt.open_partitioned_dataset(input_files, chunks={'time': 100})
    print('Large dataset loaded.')

    ## 4. Get variables and subset of complete dataset
    #----------------------------------------------------------------------------------------
    # 4.1 Get the gridname 
    gridname = ds.ugrid.grid.name

    # 4.2 Select only the relevant variables
    # > {gridname}_node_z is necessary to keep the  nodal dimension.
    ds = ds[keep_variables]
    # ds = ds[[f'{gridname}_node_z', f'{gridname}_sa1', f'{gridname}_vol1', f'{gridname}_tem1', f'{gridname}_vicwwu']] # f'{gridname}_au',

    # 4.3 Define counter for file initialization and later appending
    i = 0

    # 4.4 Loop through variables and save as netCDF file in loop
    for v in keep_variables:

        # > Select the time
        print(f'Starting selection of variable {v}...')
        vds = ds[v]
        print(f'Loaded the dataset at variable {v}.')

        if i == 0:
            # > For t = 0 in the range of timesteps, check if there's a file
            # > already present. If it is, remove it.
            if os.path.isfile(out_file):
                os.remove(out_file)

            # Use dask.delayed to write file to disk
            print(f'Writing file {out_file} to disk...')
            vds.ugrid.to_netcdf(out_file, mode='w', compute=True)

        else:
            # Use dask.delayed to write file to disk
            print(f'Writing to file {out_file} in append mode...')
            print(f"Outputfile: {out_file}")
            vds.ugrid.to_netcdf(out_file, mode='a', compute=True)

        vds.close()

        # > Update counter
        i += 1

    client.close()