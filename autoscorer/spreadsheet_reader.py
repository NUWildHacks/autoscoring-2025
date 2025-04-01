"""
CSV spreadsheet reader module that returns a DuckDB table
"""

import os
import pandas as pd

class SpreadsheetReader:
    def __init__(self, path: str):
        self.path = path
        self.df = pd.read_csv(os.path.abspath(self.path))
    
    def replace_columns(self, columns: list[str]):
        """
        Takes a pandas dataframe and replaces the column names with the given list of column names

        First asserts that len(columns) == len(df.columns)
        """
        df_columns = self.df.columns
        assert (len(columns) == len(df_columns)), "Number of columns in list does not match number of columns in dataframe"
        self.df.columns = columns

    def get_df(self):
        return self.df

