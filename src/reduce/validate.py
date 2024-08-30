from dask import delayed, compute
import xarray as xr
import warnings
import glob

def check_file(file):
    file_name = file.split('/')[-1]
    try:
        test = xr.open_dataset(file)
        return None  # If the file is fine, return None
    except:
        warnings.warn(f'The file {file_name} seems to be corrupted. Check your inputs.', UserWarning)
        return file  # Return the file name if it's corrupted

def validate_input(input_regex):
    file_list = glob.glob(input_regex)

    # Create a list of delayed tasks
    tasks = [delayed(check_file)(file) for file in file_list]

    # Compute tasks in parallel
    results = compute(*tasks)

    # Filter out None values to get only corrupted files
    corrupted_files = [file for file in results if file is not None]

    return corrupted_files

    # > Check if there are any corrupted files in the listed files
    if len(corrupted_files) > 0:
        print('There are corrupted files. These will be taken out of the file list for the next tests.')
    else:
        print('No corrupted files found.')
        
    # 2.4 Get the entire input list and remove the corrupted files
    input_files = [f for f in glob.glob(input_regex) if not f in corrupted_files]

    return input_files
