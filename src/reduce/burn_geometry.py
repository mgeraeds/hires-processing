import xugrid as xu
import dfm_tools as dfmt
import numpy as np 
import argparse
import geopandas as gpd 
import os
import dask

from dask.distributed import Client

if __name__ == '__main__':

    # > Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str, required=True)
    parser.add_argument('-o', '--out_file', type=str)  # type=dir_path)
    parser.add_argument('-bc', '--batch_cores', type=str, required=True)
    parser.add_argument('-gf', '--geometry_file', type=str, required=True)
    parser.add_argument('-crs', '--data_crs', type=str, required=True)
    args = parser.parse_args()

    # > Get arguments
    file_nc = args.file
    out_file = args.out_file
    batch_cores = args.batch_cores
    geometry_file = args.geometry_file
    data_crs = args.data_crs

    n_cores = 4
    n_processes = 4
    n_workers = n_cores * n_processes
    max_mem_alloc = 16 * int(batch_cores)  # 1.75 = 768/48 = 16
    mem_lim = str(int(np.floor(max_mem_alloc))) + 'GB'  # 336 GiB memory for genoa node

    print('Starting client...')
    client = Client(n_workers=16, threads_per_worker=1, memory_limit=mem_lim)
    client.amm.start()  # automatic memory management
    print('Started client.')

    # Open the partitioned dataset with xarray
    print('Loading large dataset...')
    # with ProgressBar():
    ds = dfmt.open_partitioned_dataset(file_nc, chunks={'time': 1, 'mesh2d_nFaces': 400000})
    print('Large dataset loaded.')

    # Set the coordinate reference system for the data
    ds = ds.set_crs(data_crs)

    # Load the polygon that will be used to burn the data
    burn_polygon = gpd.read_file(geometry_file)

    # Make sure that the polygon's crs is the same as the dataset's crs
    if ''.join('EPSG:', str(burn_polygon.crs.to_epsg())) != data_crs:
        burn_polygon.set_crs(data_crs)

    # Obtain dataset characteristics
    times = ds.time
    gridname = ds.ugrid.grid.name

    # Define counter for possible subdivision of times in writing
    i = 0

    for t in range(len(times)):
        
        # 1. Test if file exists and do tasks that only need to be executed on first timestep 
        # > Select the time
        print(f'Starting selection of timestep {t}...')
        tds = ds.isel(time=[t])
        print(f'Loaded the dataset at timestep {t}.')

        if t == 0:
            # > For t = 0 in the range of timesteps, check if there's a file
            # > already present. If it is, remove it.
            if os.path.isfile(out_file):
                os.remove(out_file)

            # > Get mask to burn geometry
            burned_geometry = xu.burn_geometry(burn_polygon, ds, all_touched=True) # todo: can change this to not automatically assume all_touched based on kwargs
        
        else:
            pass

        # 3. Computation
        # > Mask the data outside of the provided polygon
        tds = tds.where(burned_geometry.notnull(), drop=True)

        # 2. Define write task
        if t == 0:
            # > Use dask.delayed to write file to disk
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