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

"""python script to persist data to the database"""
import os

from mods.decom_clarity_transformer import DecomClarityTransformer
from mods.analyze_diabetes_db_data import AnalyzeDiabetesDbData

# initial setup
cwd = os.getcwd()
data_output = os.path.join(cwd, "output")
data_dir = os.path.join(cwd, "data")
# define the path to the Dexcom Clarity export file
decom_data_file = os.path.join(
    data_dir, "Clarity_Export_Willmore_James_2024-12-14_135610.csv"
)

# instantiate the transformer and transform the data
decom_transformer = DecomClarityTransformer()
decom_df = decom_transformer.transform(decom_data_file)

df = (
    decom_df.copy()
    .reindex()
    .sort_values(by="datetime", ascending=False)
)
df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")

diabetes_db = AnalyzeDiabetesDbData()
diabetes_db.insert_records(df)

print("DONE!")
