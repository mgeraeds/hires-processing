import xarray as xr
import warnings
import glob

def validate_input(input_regex):
    """Function to determine whether the input contains corrupted files and remove these from the input before loading.

    Args:
        input_regex (str):  The regex to the files to be loaded.
    
    Returns: 
        file_list (list):   A list of files from the input_regex that are not corrupt and can be used to create correct topology later on.
    """
    # > Get file list from the regex
    file_list = glob.glob(input_regex)
    # > Make a list for corrupted files
    corrupted = []

    for file in file_list:

        file_name = file.split('/')[-1]
        
        try:
            xr.open_dataset(file)
        except:
            warnings.warn(f'The file {file_name} seems to be corrupted. Check your inputs.', UserWarning)
            corrupted.append(file)

    # > Check if there are any corrupted files in the listed files
    if len(corrupted) > 0:
        print('There are corrupted files. These will be taken out of the file list for the next tests.')
    else:
        print('No corrupted files found.')
        
    # 2.4 Get the entire input list and remove the corrupted files
    input_files = [f for f in glob.glob(input_regex) if not f in corrupted]

    return input_files