import dfm_tools as dfmt
import argparse
import os
import dask
import glob
import warnings
from dask.distributed import Client
import glob

import xarray as xr
import xugrid as xu
import numpy as np
import json
from collections import ChainMap

# from src.reduce.validate import validate_input

# Set up class to parse dictionaries for the kwargs
# From: https://sumit-ghosh.com/posts/parsing-dictionary-key-value-pairs-kwargs-argparse-python/
class ParseKwargs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())
        for value in values:
            key, value = value.split('=')
            getattr(namespace, self.dest)[key] = value

def parse_slice(time_slice):
    """
    Function that transforms a list input to a slice.
    TODO: docstring formatted for Sphinx
    """
    if time_slice == ['all']:
        time_slice = slice(None)
        return time_slice
    try:
        time_slice = slice(*time_slice)
    except ValueError:
        if len(time_slice) > 3:
                raise ValueError('Input of time_slice argument gives incorrect slice.')
        
    return time_slice
def print_and_log(text):
    print(text)
    with open("logfile.txt", "a") as logfile:
        logfile.write(text+"\n")

def reduce_dataset(files, out_file, keep_vars, **kwargs): 
    """ 
    TODO: docstring formatted for Sphinx
    """
    ## 1. Get kwargs
    if 'chunks' in kwargs:
        chunks = kwargs['chunks']
    if 'encoding' in kwargs:
        encoding = kwargs['encoding']
    # if 'validate_input' in kwargs:
    #     validate_input = kwargs['validate_input']
    #     if validate_input == None:
    #         validate_input = False
    #     else: 
    #         validate_input = True
    if 'time_slice' in kwargs:
        time_slice = kwargs['time_slice']

    print_and_log(f'Used keyword arguments are: {kwargs}')

    ## 2. Input tests
    #----------------------------------------------------------------------------------------
    # if validate_input: 
    #     input_files = validate_input(files)
    # else:
    #     input_files = glob.glob(files)
    input_files = files

    ## 3. Open the partitioned dataset with xarray
    #----------------------------------------------------------------------------------------
    print_and_log('Loading large dataset...')
    try:
        ds = dfmt.open_partitioned_dataset(input_files, chunks=chunks)
    except:
        try:
            ds = xr.open_dataset(input_files, chunks=chunks)
        except:
            raise Exception('Could not load files. Check your inputs and/or regex.')
        
    print_and_log('Large dataset loaded.')

    ## 4. Get variables and subset of complete dataset
    #----------------------------------------------------------------------------------------
    # Define counter for possible subdivision of times in writing
    i = 0

    # > If it's an unstructured xu.UgridDataset, make the dataset into an xr.Dataset
    if isinstance(ds, xu.core.wrap.UgridDataset):
        # > We need to store the hidden information as well, so get those variables
        hidden_vars = list(ds.ugrid.grid.to_dataset().variables)
        # > Add them to the keep_vars list
        keep_vars = keep_vars + hidden_vars
        # > Make the original dataset into an xr.Dataset
        ds = ds.ugrid.to_dataset()
        # > Only get the reduced dataset with keep_vars
        ds = ds[keep_vars]
        
    else:
        ds = ds[keep_vars]

    # > Select the required time period (if indicated)
    try:
        tds = ds.isel(time=time_slice)
    except:
        try:
            tds = ds.sel(time_slice=time_slice)
        except:
            tds = ds

    for v in keep_vars:

        # > Select the time
        print_and_log(f'Starting selection of variable {v}...')
        vds = tds[v] 

        # > If there's encoding specified, make a subset for the variables in that dataset only
        var_list = list(vds.coords) + [vds.name]
        encoding_sub = dict(ChainMap(*[{f"{v}": encoding[v]} for v in var_list if v in encoding]))

        if i == 0:
            # > For t = 0 in the range of timesteps, check if there's a file
            # > already present. If it is, remove it.
            if os.path.isfile(out_file):
                os.remove(out_file)

            # Use dask.delayed to write file to disk
            print_and_log('Writing file to disk...')
            vds.to_netcdf(out_file, mode='w', compute=True, encoding=encoding_sub)

        else:
            # Use dask.delayed to write file to disk
            print_and_log('Writing file to disk in append mode...')
            vds.to_netcdf(out_file, mode='a', compute=True, encoding=encoding_sub)

        # writing_task.compute()
        # vds.close()
        print_and_log(f"Outputfile: {out_file}")
        
        # > Update counter
        i += 1


if __name__ == '__main__':
    
    # Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--files', nargs='+', type=str, required=True, help="file regex of the files to open")
    parser.add_argument('-o', '--out_file', type=str, help="the name of the output file")  # type=dir_path)
    parser.add_argument('-n', '--n_processes', type=str, required=True, help="the amount of processes to spawn with the dask client")
    parser.add_argument('-m', '--max_mem', type=int, required=True, help="the maximum memory that can be used by the dask client")
    parser.add_argument('-k', '--keep_vars', nargs='+', type=str, required=True, help="the variables to keep in the final output file")
    parser.add_argument('-c', '--chunks', nargs='*', action=ParseKwargs, help="the desired chunks, given as <dimension>=n_chunks")
    # parser.add_argument('-v', '--validate_input', default=False, type=bool, help="boolean defining whether to validate the input regex or not")
    parser.add_argument('-e', '--encoding', default={}, type=json.loads, help="dictionary defining the encoding passed to the to_netcdf call")
    parser.add_argument('-t', '--time_slice', type=parse_slice, nargs='+', default='all', help="slice or time (int or specific time) to select for the output file")

    args = parser.parse_args()

    # Get arguments
    files = args.files
    out_file = args.out_file
    max_mem = args.max_mem
    n_processes = args.n_processes
    keep_vars = args.keep_vars
    chunks = args.chunks
    # validate_input = args.validate_input
    encoding = args.encoding
    time_slice = args.time_slice

    # 1. Set up Dask client
    #----------------------------------------------------------------------------------------
    mem_lim = str(int(np.floor(max_mem))) + 'GB'

    print(f"Allocated memory to the dask client is {mem_lim}.")

    print('Starting client...')
    client = Client(n_workers=int(n_processes), memory_limit=mem_lim)
    client.amm.start() # automatic memory management

    # 2. Apply reduce_dataset function
    #----------------------------------------------------------------------------------------
    # reduce_dataset(files=files, out_file=out_file, keep_vars=keep_vars, chunks=chunks, encoding=encoding, time_slice=time_slice)
    input_files = files

    ## 3. Open the partitioned dataset with xarray
    #----------------------------------------------------------------------------------------
    print('Loading large dataset...')
    try:
        ds = dfmt.open_partitioned_dataset(input_files, chunks=chunks)
    except:
        try:
            ds = xr.open_dataset(input_files, chunks=chunks)
        except:
            raise Exception('Could not load files. Check your inputs and/or regex.')
        
    print('Large dataset loaded.')

    ## 4. Get variables and subset of complete dataset
    #----------------------------------------------------------------------------------------
    # Define counter for possible subdivision of times in writing
    i = 0

    # > If it's an unstructured xu.UgridDataset, make the dataset into an xr.Dataset
    if isinstance(ds, xu.core.wrap.UgridDataset):
        # > We need to store the hidden information as well, so get those variables
        hidden_vars = list(ds.ugrid.grid.to_dataset().variables)
        # > Add them to the keep_vars list
        keep_vars = keep_vars + hidden_vars
        # > Make the original dataset into an xr.Dataset
        ds = ds.ugrid.to_dataset()
        # > Only get the reduced dataset with keep_vars
        ds = ds[keep_vars]
        
    else:
        ds = ds[keep_vars]

    # > Select the required time period (if indicated)
    try:
        tds = ds.isel(time=time_slice)
    except:
        try:
            tds = ds.sel(time_slice=time_slice)
        except:
            tds = ds

    for v in keep_vars:

        # > Select the time
        print(f'Starting selection of variable {v}...')
        vds = tds[v] 

        # > If there's encoding specified, make a subset for the variables in that dataset only
        var_list = list(vds.coords) + [vds.name]
        encoding_sub = dict(ChainMap(*[{f"{v}": encoding[v]} for v in var_list if v in encoding]))

        if i == 0:
            # > For t = 0 in the range of timesteps, check if there's a file
            # > already present. If it is, remove it.
            if os.path.isfile(out_file):
                os.remove(out_file)

            # Use dask.delayed to write file to disk
            print('Writing file to disk...')
            vds.to_netcdf(out_file, mode='w', compute=True, encoding=encoding_sub)

        else:
            # Use dask.delayed to write file to disk
            print('Writing file to disk in append mode...')
            vds.to_netcdf(out_file, mode='a', compute=True, encoding=encoding_sub)

        # writing_task.compute()
        vds.close()

        print(f"Outputfile: {out_file}")
        # > Update counter
        i += 1

    # 3. Close the client
    #----------------------------------------------------------------------------------------
    client.close()