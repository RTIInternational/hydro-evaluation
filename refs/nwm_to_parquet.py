# Copyright 2022 Google LLC.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam
from apache_beam.internal import pickler
import datetime
import fsspec
import numpy as np
import xarray as xr
import pandas as pd
import os
import tempfile
import argparse
import logging
import google.auth
from google.cloud import storage
from google.auth import compute_engine
from itertools import groupby
import re


pickler.set_library(pickler.USE_CLOUDPICKLE)

# global variable for NWM bucket id
BUCKET_NAME = 'national-water-model'

# configuration options for different model runs
CONFIG_OPTS = [
    'analysis_assim',
    'analysis_assim_no_da',
    'analysis_assim_extend',
    'analysis_assim_extend_no_da',
    'analysis_assim_long',
    'analysis_assim_long_no_da',
    'short_range', 
    'medium_range_mem1',
    'medium_range_mem2',
    'medium_range_mem3',
    'medium_range_mem4',
    'medium_range_mem5',
    'medium_range_mem6',
    'medium_range_mem7',
    'medium_range',
    'medium_range_no_da',
    'long_range_mem1', 
    'long_range_mem2', 
    'long_range_mem3', 
    'long_range_mem4'
]

# output type options
TYPE_OPTS = ['channel_rt', 'reservoir']


def decode_date(date):
    """Decodes a date string input to a datetime object".

    args:
        date (str): date value in a format that can be parsed into datetime object

    returns:
        datetime.datetime: decoded datetime value

    raises:
        TypeError: if string does not conform to a legal date format.
    """

    date_formats = [
        "%Y%m%d",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for date_format in date_formats:
        try:
            dt = datetime.datetime.strptime(date, date_format)
            return dt
        except ValueError:
            continue
    raise TypeError(f"Invalid value for property of type 'date': '{date}'.")


def make_daterange(start_date, end_date):
    """Helper function to create a list of dates from a start and end date"""
    dateindex = pd.date_range(start=start_date, end=end_date)
    dates = dateindex.strftime('%Y-%m-%d').to_list()
    return dates


def list_objects(inputs) -> list[str]:
    """Function to list NWM URIs based on date, file type, and model config"""

    date, type, config = inputs

    storage_client = storage.Client()

    if type not in TYPE_OPTS:
        raise ValueError(
            'type argument is not valid. options are "channel_rt" or "reservoir"')

    

    if config not in CONFIG_OPTS:
        raise ValueError('config argument is not valid.')

    if not isinstance(date, datetime.datetime):
        date = decode_date(date)

    datestr = date.strftime('%Y%m%d')

    folder = f'nwm.{datestr}'
    prefix = f'{folder}/{config}/'

    blobs = storage_client.list_blobs(
        BUCKET_NAME, prefix=prefix, delimiter='/')
    blob_names = [blob.name for blob in blobs if type in blob.name]

    return blob_names


def load_dataset(filename, engine='h5netcdf') -> xr.Dataset:
    """Load a NetCDF dataset from local file system or cloud bucket."""

    uriname = f'gs://{BUCKET_NAME}/{filename}'
    with fsspec.open(uriname, mode="rb") as file:
        ds = xr.load_dataset(file, engine=engine)

    if 'mem' in filename:
        ensemble_id = int(uriname.split('/')[-2].split('_')[-1][-1]) - 1
    else:
        ensemble_id = 0

    ds.attrs['ensemble_id'] = ensemble_id

    ds['forecast_offset'] = (
        (ds['time']-ds['reference_time'])/(60*60*1e9)).astype(np.int8)

    return (ds, filename)


def transform(dataset):
    """Function to run transformation a multi-dim dataset to a tabular structure"""

    # convert xarray Dataset to pandas DataFrame and fill missing values with 0
    df = dataset[0].to_dataframe().fillna(-999)
    df['ensemble'] = dataset[0].attrs['ensemble_id']

    # testing pipeline with only a few rows per file
    # df = df.iloc[0:100]

    return (df, dataset[1])


def write_parq(table, outbucket=None):
    """Function to write pandas dataframe as parquet file on GCS"""
    tbl, filename = table

    # do some string/list finagling to get the final output name
    filename_components = filename.split('/')
    dateinfo = filename_components[0].split('.')[-1]
    name_components = filename_components[-1].split('.')
    components_list = name_components[0:1] + \
        [dateinfo, ] + name_components[1:-1] + ['parq', 'gz']
    name = '.'.join(components_list)

    uriname = f'{outbucket}/{name}'

    with fsspec.open(uriname, mode="wb") as file:
        tbl.to_parquet(file, compression='gzip')

    return


def run_job(type: str, config: str, outbucket: str, date=None, daterange=None, runner=None, pipeline_args=None):

    if (date is None) and (daterange is not None):
        dates = make_daterange(*daterange)
        date = dates[0]

    elif (date is not None) and (daterange is None):
        dates = [date, ]

    elif (date is not None) and (daterange is not None):
        logging.warn(
            'Both date and daterange arguments provided. Using daterange...')
        dates = make_daterange(*daterange)
        date = dates[0]

    else:
        raise ValueError(
            'Either date or daterange arguments must be provided to execute job')

    logging.debug(dates)

    if not outbucket.endswith('/'):
        outbucket += '/'

    productbucket = f'{outbucket}{type}/{config}'

    args = [(date, type, config) for date in dates]

    nested_sources = map(list_objects, args)
    sources = [item for sublist in nested_sources for item in sublist]
    nfiles = len(sources)
    logging.info(f'Found {nfiles} files to process')

    if nfiles == 0:
        raise RuntimeError(f"Found {nfiles} files for {date} using type='{type}' and config='{config}' options\n"
                            "please make sure the comination of date, config, and output type is valid")


    dtstr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    logging.debug(sources)

    # check to make sure the proper options and runner type are provided
    if pipeline_args is not None and runner == 'DataflowRunner':
        # Define the pipeline options
        opts = beam.pipeline.PipelineOptions(
            flags=pipeline_args,
            job_name=f'nwm-{type.replace("_","")}-{config.replace("_","")}-{date.replace("-","")}-{dtstr}',
            pickle_library='cloudpickle',
            requirements_file='requirements.txt',
        )

    # raise error if trying to run dataflow job without proper options
    elif pipeline_args is None and runner == 'DataflowRunner':
        raise RuntimeError(
            'Must provide DataflowRunner arguments when using that runner option')

    else:
        opts = None

    # define the pipeline and run
    with beam.Pipeline(runner=runner, options=opts) as p:
        (p
            | 'Input URIs' >> beam.Create(sources)
            | 'Load data' >> beam.Map(load_dataset)
            | 'Data transfrom' >> beam.Map(transform)
            | 'Write' >> beam.Map(write_parq, outbucket=productbucket)
         )

    return


def main():
    """Main entry point; defines and runs the data transform for NWM tabular data."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--type',
                        type=str,
                        required=True,
                        help="NWM file type to read",
                        choices=TYPE_OPTS
                        )

    parser.add_argument('--config',
                        type=str,
                        required=True,
                        choices=CONFIG_OPTS,
                        help="NWM model configuration to read"
                        )

    parser.add_argument('--outbucket',
                        type=str,
                        required=True,
                        help="output storage bucket to save results to",
                        )

    parser.add_argument('--date',
                        type=str,
                        default=None,
                        help="Date of NWM data to apply transformation on",
                        )

    # date range
    parser.add_argument('--daterange',
                        type=str,
                        nargs='+',
                        default=None,
                        help="Date range of NWM data to apply transformation on",
                        )

    parser.add_argument('--runner',
                        type=str,
                        required=True,
                        default='DirectRunner',
                        choices=['DirectRunner', 'DataflowRunner'],
                        help='Run the pipeline locally or via Dataflow'
                        )

    args, pipeline_args = parser.parse_known_args()
    logging.debug(pipeline_args)
    logging.debug(args)

    run_job(args.type, args.config, args.outbucket, date=args.date, daterange=args.daterange,
            runner=args.runner, pipeline_args=pipeline_args)

    return


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()