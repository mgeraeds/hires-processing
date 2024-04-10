import xarray as xr
import xugrid as xu
import argparse
import numpy as np
import dfm_tools as dfmt
import warnings
from dask.distributed import Client
import glob

# > Set up class to parse dictionaries for the kwargs
# > From: https://sumit-ghosh.com/posts/parsing-dictionary-key-value-pairs-kwargs-argparse-python/
class ParseKwargs(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())
        for value in values:
            key, value = value.split('=')
            getattr(namespace, self.dest)[key] = value


if __name__ == '__main__':

    # Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-if', '--input_files', type=str, required=True)
    parser.add_argument('-of', '--output_file', type=str, required=True)
    parser.add_argument('-k', '--kwargs', nargs='*', action=ParseKwargs)
    parser.add_argument('-c', '--chunks', nargs='*', action=ParseKwargs)
    parser.add_argument('-bc', '--batch_cores', type=str, required=True)
    args = parser.parse_args()

    # Get kwargs
    input_files = args.input_files
    output_file = args.output_file # put in a regex statement
    kwargs = args.kwargs
    chunks = args.chunks
    batch_cores = args.batch_cores

    # Set up the dask client
    # Initialization of cores
    n_cores = 4
    n_processes = 4
    n_workers = n_cores * n_processes
    max_mem_alloc = 1.75 * int(batch_cores) #1.75 = 336/192
    mem_lim = str(int(np.floor(max_mem_alloc)))+'GB' #336 GiB memory for genoa node
    
    # > Start client
    print('Starting client...')
    client = Client(n_workers=n_workers, threads_per_worker=1, memory_limit=mem_lim)
    client.amm.start() #automatic memory management
    print('Started client.')
    
    # > If chunking is not given, use the standard of 40 timesteps per chunk
    if not chunks:
        chunks = {"time":40}

    ## 1. Input tests
    # 1.x. Loop through all of the files and write when there is something wrong with the file
    file_list = glob.glob(input_files)

    # 1.x Make a list for corrupted files
    corrupted = []

    for file in file_list:
        file_name = file.split('/')[-1]
        try:
            test = xr.open_dataset(file)
        except:
            warnings.warn(f'The file {file_name} seems to be corrupted. Check your inputs.', UserWarning)
            corrupted.append(file)

    # I.x Check if there are any corrupted files in the listed files
    if len(corrupted) > 0:
        print('There are corrupted files. These will be taken out of the file list for the next tests.')
    # I.x Get the entire input list and remove the corrupted files
    input_files = glob.glob(input_files)
    

    # 2.x. Read the total input file to check topology errors
    uds_in = dfmt.open_partitioned_dataset(input_files, chunks=chunks)

    ## 2. Output tests
    # 2.1. Read the output file
    uds_out = dfmt.open_partitioned_dataset(output_file, chunks=chunks)

    # 2.2. Check if all time steps were written
    uds_out.time
    

