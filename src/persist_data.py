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
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy.sql import text

from mods.decom_clarity_transformer import DecomClarityTransformer


# initial setup
load_dotenv()
cwd = os.getcwd()
server = os.getenv('server')
database = os.getenv('database')
username = os.getenv('username')
password = os.getenv('password')
schema = os.getenv('schema')
connection = sa.create_engine(f"postgresql://{username}:{password}@{server}/{database}")
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


def custom_insert(table: str, conn: sa.Engine, keys: list, data_iter: iter):
    """This is a custom insert into database function that will insert data into the database.

    Args:
        table (str): database table name
        conn (sa.Engine): SQLAlchemy engine
        keys (list): list of keys (database columns)
        data_iter (iter):  iterable that iterates the values to be inserted
    """
    insert_stmt = f"INSERT INTO {table.schema}.{table.name} ({', '.join(keys)}) VALUES "

    values = [
        f"({', '.join(map(repr, row))})" 
        for row in data_iter
    ]
    
    for value in values:
        full_insert = insert_stmt + value + " ON CONFLICT DO NOTHING"
        
        conn.execute(text(full_insert))
        
# insert data into the database using the custom insert function
df.to_sql(
    "diabetes_data",
    connection,
    schema=schema,
    if_exists="append",
    index=False,
    method=custom_insert,
)

print("DONE!")
