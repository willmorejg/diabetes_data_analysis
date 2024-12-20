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

import unittest
import os

from test_setup import TestSetup
TestSetup()

from mods.decom_clarity_transformer import DecomClarityTransformer
from mods.analyze_diabetes_db_data import AnalyzeDiabetesDbData

class TestAnalyzeDiabetesDbData(unittest.TestCase):
    # @unittest.skip('punt')
    def test_insert_date(self):
        # initial setup
        cwd = os.getcwd()
        data_dir = os.path.join(cwd, "data")
        # define the path to the Dexcom Clarity export file
        decom_data_file = os.path.join(
            data_dir, ""
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
        self.assertTrue(True)
    
    # @unittest.skip('punt')
    def test_dataframes(self):
        diabetes_db = AnalyzeDiabetesDbData()
        df = diabetes_db.read_data_days_from_now(90)
        self.print_result("df", df)
        self.assertIsNotNone(df)
        
        df_carbs = diabetes_db.create_carbs_dataframe(df)
        avg_carbs = df_carbs.carbs.mean()
        self.print_result("avg_carbs", avg_carbs)
        self.assertGreater(avg_carbs, 0)
        
        df_glucose = diabetes_db.create_glucose_dataframe(df)
        avg_glucose = df_glucose.glucose.mean()
        self.print_result("avg_glucose", avg_glucose)
        self.assertGreater(avg_glucose, 0)
        
        df_bolus = diabetes_db.create_bolus_dataframe(df)
        avg_bolus = df_bolus.bolus.mean()
        self.print_result("avg_bolus", avg_bolus)
        self.assertGreater(avg_bolus, 0)
        
        df_basal = diabetes_db.create_basal_dataframe(df)
        avg_basal = df_basal.basal.mean()
        self.print_result("avg_basal", avg_basal)
        self.assertGreater(avg_basal, 0)
        
        tdd = diabetes_db.calculate_total_daily_dose(df)
        self.print_result("tdd", tdd)
        self.assertGreater(tdd, 0)
        
        isf = diabetes_db.calculate_insulin_sensitivity_factor(df)
        self.print_result("isf", isf)
        self.assertGreater(isf, 0)
        
        df_target_deviation = diabetes_db.add_target_deviation_to_dataframe(df)
        df_target_deviation_1 = df_target_deviation[(df_target_deviation.target_deviation > 0.0)]
        self.print_result("df_target_deviation_1", df_target_deviation_1)
        self.assertIsNotNone(df_target_deviation_1)
        
        df_bolus_ratio = diabetes_db.add_bolus_ratio_to_dataframe(df_target_deviation)
        df_bolus_ratio = df_bolus_ratio[(df_bolus_ratio.bolus_ratio > 0.0)]
        self.print_result("df_bolus_ratio", df_bolus_ratio)
        self.assertIsNotNone(df_bolus_ratio)
        
        df_new_ratio = diabetes_db.recalculate_bolus_ratio(df)
        df_new_ratio = df_new_ratio[(df_new_ratio.bolus_ratio > 0.0)]
        self.print_result("df_new_ratio", df_new_ratio)
        self.assertIsNotNone(df_new_ratio)
        
    def print_result(self, name, result):
        print("\n====================\n", name, "\n<><><><><>\n", result, "\n====================\n")
if __name__ == '__main__':
    unittest.main()