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

import pandas as pd
from .file_transformer import FileTransformer

class DecomClarityTransformer(FileTransformer):
    """Transforms a Clarity CSV file into a DataFrame.

    Args:
        FileTransformer (_type_): _description_
    """
    def _read_into_dataframe(self, file):
        return pd.read_csv(file).dropna(subset=["Timestamp (YYYY-MM-DDThh:mm:ss)"])

    def columns(self):
        """Enumerates the columns that are required for the transformer."""
        
        return [
            "Index",
            "Timestamp (YYYY-MM-DDThh:mm:ss)",
            "Event Type",
            "Event Subtype",
            "Glucose Value (mg/dL)",
            "Insulin Value (u)",
            "Carb Value (grams)",
        ]
    
    def _populate_bolus_insulin(self, row: pd.Series) -> float:
        """Populate the bolus insulin column.

        Args:
            row (pd.Series): pandas Series (DataFrame row)

        Returns:
            float: bolus insulin value
        """
        if row["Event Type"] == "Insulin" and row["Event Subtype"] == "Fast-Acting":
            return row["insulin"]
        else:
            return 0
    
    def _populate_basal_insulin(self, row: pd.Series) -> float:
        """ Populates the basal insulin column.

        Args:
            row (pd.Series): pandas Series (DataFrame row)

        Returns:
            float: basal insulin value
        """
        if row["Event Type"] == "Insulin" and row["Event Subtype"] == "Long-Acting":
            return row["insulin"]
        else:
            return 0
    
    def _transform_glucose(self, row: pd.Series) -> float:
        """Transforms the glucose value. Dexcom Clarity uses "High" and "Low" as values when the glucose value is out-of-range.
        This function transforms those values to 400.0 and 40.0 respectively.
        
        Args:
            row (pd.Series): pandas Series (DataFrame row)
            
        Returns:
            float: glucose value
        """
        if row['glucose'] == "High":
            return 400.0
        elif row['glucose'] == "Low":
            return 40.0
        else:
            return float(row['glucose'])
    
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renames the columns in the DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame to be transformed
            
        Returns:
            pd.DataFrame: transformed DataFrame
        """
        df = df.rename(
            columns={
                "Timestamp (YYYY-MM-DDThh:mm:ss)": "datetime",
                "Glucose Value (mg/dL)": "glucose",
                "Insulin Value (u)": "insulin",
                "Carb Value (grams)": "carbs",
            }
        )
        return df

    def _additional_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Additional transformations to the DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame to be transformed
            
        Returns:
            pd.DataFrame: transformed DataFrame
        """
        df["bolus"] = df.apply(self._populate_bolus_insulin, axis=1)
        df["basal"] = df.apply(self._populate_basal_insulin, axis=1)
        df["glucose"] = df.apply(self._transform_glucose, axis=1)
        df = df.fillna(0)
        df = df[['datetime', 'glucose', 'carbs', 'bolus', 'basal', 'hour', 'hour_group']]
        return df
