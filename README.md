# Hydro-evaluation
Repository for testing research code related to the CIROH Evaluation System project.  Expect to find broken and incomplete code.

# Development Environment
Most of the code in this repo, particularly the Jupyter Notebooks, was written and run on a Pangeo [https://pangeo.io/] base image of JupyterHub. In most cases if addition Python packages are required to ruin a particular notebook, they are installed using the `!pip install duckdb` pattern within the notebook.

Git LFS is used for large files (*.csv, *.nc, etc.)

`nbstripoutput` is configured to strip output from notebooks to keep the size down and make diffing files easier.  See https://github.com/kynan/nbstripout.
Note, after cloning, you must run `nbstripout --install` in the repo to install `nbstripoutput`.
The configuraion is stored in the `.gitattributes` file, but the tool must be installed per repo.
