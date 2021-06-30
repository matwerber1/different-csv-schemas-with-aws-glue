import os
import pandas as pd
import pyarrow as pa
from pyarrow import csv, parquet
from datetime import datetime



def file_to_data_frame_to_parquet(local_file: str, parquet_file: str) -> None:
    table = pa.csv.read_csv(local_file)
    pa.parquet.write_table(table, parquet_file)


if __name__ == "__main__":
    
    csv_directory = 'data/raw/csv/transactions'
    parquet_directory = 'data/raw/parquet/transactions'
    tables = []

    opts = csv.ParseOptions(delimiter='|')

    for csv_filename in os.listdir(csv_directory):
        # read CSV file into arrow table
        filename_without_ext = os.path.splitext(csv_filename)[0]
        csv_filepath = os.path.join(csv_directory, csv_filename)
        table = pa.csv.read_csv(csv_filepath, parse_options=opts)
        tables.append(table)

        # convert to parquet file
        parquet_filename = filename_without_ext + ".parquet"
        parquet_filepath = os.path.join(parquet_directory, parquet_filename)
        parquet.write_table(table, parquet_filepath)


