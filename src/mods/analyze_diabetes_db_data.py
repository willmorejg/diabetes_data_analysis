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

import os
from dotenv import load_dotenv
import datetime as dt
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql import text


class AnalyzeDiabetesDbData:
    sql = """
    SELECT "hour", hour_group, datetime, glucose, carbs, bolus, basal 
    FROM diabetes.diabetes_data
    """

    def __init__(self, init_connection: bool = True):
        """Initialize the class

        Args:
            init_connection (bool): flag to initialize the database connection
        """
        load_dotenv()

        if init_connection:
            self.init_database_connection_from_env()

    def init_database_connection_from_env(self):
        """Initialize the database connection from the environment variables"""
        server = os.getenv("server")
        database = os.getenv("database")
        username = os.getenv("username")
        password = os.getenv("password")
        self.connection = sa.create_engine(
            f"postgresql://{username}:{password}@{server}/{database}"
        )

    def get_schema(self):
        """Get the schema from the environment variables"""
        return os.getenv("schema")

    def get_table(self):
        """Get the table from the environment variables"""
        return os.getenv("table")

    def insert_records(self, df: pd.DataFrame):
        """Insert records into the database

        Args:
            df (pd.DataFrame): pandas DataFrame to be inserted into the database
        """
        schema = self.get_schema()
        table = self.get_table()
        df.to_sql(
            table,
            self.connection,
            schema=schema,
            if_exists="append",
            index=False,
            method=self._custom_insert_records,
        )

    ## TODO - make SQL database agnostic
    def _custom_insert_records(
        self, table: str, conn: sa.Engine, keys: list, data_iter: iter
    ):
        """This is a custom insert into database function that will insert data into the database.

        Args:
            table (str): database table name
            conn (sa.Engine): SQLAlchemy engine
            keys (list): list of keys (database columns)
            data_iter (iter):  iterable that iterates the values to be inserted
        """
        insert_stmt = (
            f"INSERT INTO {table.schema}.{table.name} ({', '.join(keys)}) VALUES "
        )

        values = [f"({', '.join(map(repr, row))})" for row in data_iter]

        for value in values:
            full_insert = insert_stmt + value + " ON CONFLICT DO NOTHING"

            conn.execute(text(full_insert))

    def read_all_data(self) -> pd.DataFrame:
        """Read all data

        Returns:
            pd.DataFrame: all diabetes data
        """

        return pd.read_sql_query(sql=self.sql, con=self.connection)

    def read_data_days_from_now(self, days_back: int) -> pd.DataFrame:
        """Read data from last given days range (ex. last 90 days).

        Args:
            days_back (int): set date range from today going back the given days

        Returns:
            pd.DataFrame: the records meeting search criteria
        """
        modified_sql = (
            f"{self.sql} WHERE datetime >= current_date - interval '{days_back} days'"
        )

        return pd.read_sql_query(sql=modified_sql, con=self.connection)

    def create_carbs_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create a DataFrame of only carbs data from given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing diabetes data

        Returns:
            pd.DataFrame: a DataFrame containing only carbs records
        """
        return df[(df.carbs > 0)]

    def create_glucose_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create a DataFrame of only glucose data from given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing diabetes data

        Returns:
            pd.DataFrame: a DataFrame containing only glucose records
        """
        return df[(df.glucose > 0)]

    def create_bolus_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create a DataFrame of only bolus data from given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing diabetes data

        Returns:
            pd.DataFrame: a DataFrame containing only bolus records
        """
        return df[(df.bolus > 0)]

    def create_basal_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create a DataFrame of only basal data from given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing diabetes data

        Returns:
            pd.DataFrame: a DataFrame containing only basal records
        """
        return df[(df.basal > 0)]
    
    def calculate_total_daily_dose(self, df: pd.DataFrame) -> float:
        """Calculate the total daily dose.

        TDD = (total daily dose) = (total basal insulin) + (total bolus insulin)

        Args:
            df (pd.DataFrame): the DataFrame containing all diabetes data

        Returns:
            float: the calculated total daily dose
        """
        basal_avg = self.create_basal_dataframe(df).basal.mean()
        bolus_avg = self.create_bolus_dataframe(df).bolus.mean()
        return basal_avg + bolus_avg

    def calculate_insulin_sensitivity_factor(self, df: pd.DataFrame) -> float:
        """Calculate the insulin sensitivity factor.

        references:
            https://www.diabetesqualified.com.au/insulin-sensitivity-factor-explained/
            https://www.mysugr.com/en/blog/insulin-sensitivity-or-insulin-correction-factor-calculating-and-testing-made-easy

        ISF = 1800 / TDD
        TDD = (total daily dose) = (total basal insulin) + (total bolus insulin)

        Args:
            df (pd.DataFrame): the DataFrame containing all diabetes data

        Returns:
            float: the calculated insulin sensitivity factor
        """
        tdd = self.calculate_total_daily_dose(df)
        return 1800 / tdd

    def calculate_target_deviation(
        self, row: dict, df_glucose: pd.DataFrame, isf: float, target: float = 120.0
    ) -> pd.DataFrame:
        """Calculate the target deviation (possible correction bolus)

        Target Deviation = (mean glucose - target) / ISF

        Args:
            row (dict): the row of data to analyze
            df_glucose (pd.DataFrame): the DataFrame containing glucose data
            isf (float): the insulin sensitivity factor
            target (float): the target glucose level

        Returns:
            float: the calculated target deviation
        """
        if row["carbs"] > 0:
            temp_dt = row.datetime + dt.timedelta(hours=2, minutes=30)
            minutes_before_5 = temp_dt - dt.timedelta(minutes=30)
            minutes_after_5 = temp_dt + dt.timedelta(minutes=30)
            temp_glucose_df = df_glucose[
                (df_glucose.datetime >= minutes_before_5)
                & (df_glucose.datetime <= minutes_after_5)
            ]

            if temp_glucose_df.empty:
                return 0.0

            temp_mean = temp_glucose_df.glucose.mean()
            value = (temp_mean - target) / isf
            return value
        return 0.0

    def add_target_deviation_to_dataframe(
        self, df: pd.DataFrame, isf: float = 0.0, target: float = 120.0
    ) -> pd.DataFrame:
        """Add the target deviation to the DataFrame.

        Will calculate ISF if left as 0.0.

        Args:
            df (pd.DataFrame): the DataFrame containing all diabetes data
            isf (float): the insulin sensitivity factor (default 0.0)
            target (float): the target glucose level (default 120.0)

        Returns:
            pd.DataFrame: the DataFrame with the target deviation added
        """
        if isf == 0:
            isf = self.calculate_insulin_sensitivity_factor(df)

        df_cpy = df.copy()
        df_cpy["target_deviation"] = df_cpy.apply(
            lambda x: self.calculate_target_deviation(
                x, self.create_glucose_dataframe(df), isf, target
            ),
            axis=1,
        )

        return df_cpy

    def calculate_bolus_ratio(self, row: dict, df_bolus: pd.DataFrame) -> float:
        """Calculate the actual bolus ratio

        Bolus Ratio = Carbs / Bolus

        Args:
            row (dict): the row of data to analyze
            df_bolus (pd.DataFrame): the DataFrame containing bolus data

        Returns:
            float: the calculated bolus ratio
        """
        if row["carbs"] > 0:
            temp_carbs = row.carbs
            temp_dt = row.datetime
            minutes_before_15 = temp_dt - dt.timedelta(minutes=15)
            minutes_after_15 = temp_dt + dt.timedelta(minutes=15)
            temp_bolus_df = df_bolus[
                (df_bolus.datetime >= minutes_before_15)
                & (df_bolus.datetime <= minutes_after_15)
            ]

            if temp_bolus_df.empty:
                return 0.0

            temp_mean = temp_bolus_df.bolus.mean()
            value = temp_carbs / temp_mean
            return value
        return 0.0

    def add_bolus_ratio_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add the bolus ratio to the DataFrame.

        Args:
            df (pd.DataFrame): the DataFrame containing all diabetes data

        Returns:
            pd.DataFrame: the DataFrame with the bolus ratio added
        """
        df_cpy = df.copy()
        df_cpy["bolus_ratio"] = df_cpy.apply(
            lambda x: self.calculate_bolus_ratio(x, self.create_bolus_dataframe(df)),
            axis=1,
        )

        return df_cpy

    def recalculate_bolus_ratio(
        self, df: pd.DataFrame, isf: float = 0.0, target: float = 120.0
    ) -> pd.DataFrame:
        """Recalculate the bolus ratio

        Args:
            df (pd.DataFrame): the DataFrame containing all diabetes data
            isf (float): the insulin sensitivity factor (default 0.0)
            target (float): the target glucose level (default 120.0)

        Returns:
            pd.DataFrame: the DataFrame with the new bolus ratio
        """
        df_cpy = df.copy()
        df_cpy = self.add_bolus_ratio_to_dataframe(df_cpy)
        df_cpy = self.add_target_deviation_to_dataframe(df_cpy, isf, target)
        df_cpy["new_ratio"] = (
            df_cpy["carbs"]
            / ((df_cpy["carbs"] / df_cpy["bolus_ratio"]) + df_cpy["target_deviation"])
        )
        df_cpy["new_ratio"] = df_cpy["new_ratio"].fillna(0)
        return df_cpy
