# WEEVAT - Water Evaluation, Exploration, Visualization and Analysis Tool-set
WEEVAT (pronounced "we-vat") is a python tool set for loading, storing, processing and visualizing water data, particularly National Water Model data, for the purpose of exploring and evaluating the datasets to assess their skill and performance.

NOTE: THIS PROJECT IS UNDER DEVELOPMENT - EXPECT TO FIND BROKEN AND INCOMPLETE CODE.

Project structure:

- `/docs`: This directory contains documentation
- `/examples`: This directory contains
- `/src`: This directory contains
- `/references`: This directory contains
- `/tests`: This directory contains
- `/weevat`: This directory contains the library source code.
- `/playground`: This directory contains
- `/dashboards`: This directory contains

This project does:
- focus on a limited number of high-performing approaches

This project does not:
- attempt to be a "Swiss army knife" of evaluation

## Development Environment
Most of the code in this repo, particularly the Jupyter Notebooks, was written and run on a Pangeo [https://pangeo.io/] base image on JupyterHub. In most cases if additional Python packages are required to run a particular notebook, they are installed using the `!pip install duckdb` pattern within the notebook.

## GIT LFS
Git LFS is used for large files (*.csv, *.nc, etc.)

## NB Strip Output
`nbstripoutput` is configured to strip output from notebooks to keep the size down and make diffing files easier.  See https://github.com/kynan/nbstripout.
Note, after cloning, you must run `nbstripout --install` in the repo to install `nbstripoutput`.
The configuraion is stored in the `.gitattributes` file, but the tool must be installed per repo.
