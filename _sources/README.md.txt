# Hi-resolution model processing toolbox
## User documentation
More in-depth documentation can be found in the [readthedocs](https://mgeraeds.github.io/hires-processing).

### Installation

**Prerequisites**

* Python 3.9 - 3.11 (Python 3.11 is recommended)
* Anaconda (recommended)

**Dependencies**
The main dependencies are:

* [dfm_tools](https://github.com/Deltares/dfm_tools?tab=readme-ov-file)
* [xugrid](https://github.com/Deltares/xugrid)
* [dask](https://github.com/dask/dask)
* [xarray](https://github.com/pydata/xarray)

We recommend installing all dependencies in a new conda environment to avoid clashing of dependencies. 

    ``
    conda create --name hires_env python=3.11
    conda activate hires_env
    pip install dfm_tools
    pip install git+https://github.com/mgeraeds/hires-processing.git
    ``

## Licensing and Attribution

This repository is licensed under the [MIT License]. You are generally free to reuse or extend upon this code as you see fit; just include the original copy of the license (which is preserved when you "make a template"). While it's not necessary, we'd love to hear from you if you do use this template, and how we can improve it for future use!

The deployment GitHub Actions workflow is heavily based on GitHub's mixed-party [starter workflows]. A copy of their MIT License is available in [actions/starter-workflows].
