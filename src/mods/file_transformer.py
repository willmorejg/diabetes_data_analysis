# Copyright 2024 James G Willmore
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABC, abstractmethod
import pandas as pd


class FileTransformer(ABC):
    """Abstract implementation of a file transformer - transform various diabetes data collection formats into a standard formet."""

    def __init__(self):
        """Initializes the FileTransformer class."""
        self._hours_grouping_dict, self._hours_grp_lookup_dict = self.get_hour_grouping_dict()
        # self._hours_grouping_dict = {}
        # self._hours_grp_lookup_dict = {}
        # cnt = 0
        # for k in range(0, 8, 1):
        #     self._hours_grouping_dict[k] = []
        #     for v in range(0, 3, 1):
        #         self._hours_grouping_dict[k].append(cnt)
        #         self._hours_grp_lookup_dict[cnt] = k
        #         cnt = cnt + 1

    @staticmethod
    def get_hour_grouping_dict():
        _hours_grouping_dict = {}
        _hours_grp_lookup_dict = {}
        cnt = 0
        for k in range(0, 8, 1):
            _hours_grouping_dict[k] = []
            for v in range(0, 3, 1):
                _hours_grouping_dict[k].append(cnt)
                _hours_grp_lookup_dict[cnt] = k
                cnt = cnt + 1
        return _hours_grouping_dict, _hours_grp_lookup_dict

    @abstractmethod
    def columns(self):
        """Enumerates the columns that are required for the transformer."""
        pass

    def _read_into_dataframe(self, file: str) -> pd.DataFrame:
        """Read the given file into a DataFrame.

        Args:
            file (str): the file to read

        Returns:
            pd.DataFrame: pandas DataFrame
        """
        return pd.read_csv(file)

    def _retrieve_columns(self, df):
        df = df[[c for c in self.columns() if c in df.columns]]
        return df

    @abstractmethod
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename the columns in the DataFrame - implemented in subclass.

        Args:
            df (pd.DataFrame): the DataFrame prior to renaming

        Returns:
            pd.DataFrame: the DataFrame with the renamed columns
        """
        pass

    def _set_datetime_datatype(self, df: pd.DataFrame) -> pd.DataFrame:
        """Set the datetime column to a datetime datatype.

        Args:
            df (pd.DataFrame): the DataFrame to set the datetime datatype

        Returns:
            pd.DataFrame: the DataFrame with the datetime column set to datetime datatype
        """
        if "datetime" in df.columns:
            df.datetime = pd.to_datetime(df.datetime)
        return df

    def _add_hr_and_hrgrp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add hour and hour group columns to the DataFrame.

        Args:
            df (pd.DataFrame): the DataFrame where the columns will be added

        Returns:
            pd.DataFrame: the DataFrame with the added columns
        """
        df["hour"] = df.datetime.dt.hour
        df["hour_group"] = df.hour.apply(lambda x: self._hours_grp_lookup_dict[x])
        return df

    @abstractmethod
    def _additional_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform additional transformation on the given DataFrame (optional - will just return DataFrame if not implemented by subclass).

        Args:
            df (pd.DataFrame): pandas DataFrame

        Returns:
            pd.DataFrame: pandas DataFrame
        """
        return df

    def transform(self, file: str) -> pd.DataFrame:
        """Transformation factory steps.

        Args:
            file (str): the file to read

        Returns:
            pd.DataFrame: pandas DataFrame
        """
        df = self._read_into_dataframe(file)
        df = self._retrieve_columns(df)
        df = self._rename_columns(df)
        df = self._set_datetime_datatype(df)
        df = self._add_hr_and_hrgrp(df)
        df = self._additional_transform(df)
        return df
