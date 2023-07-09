import unittest
import os

import pandas as pd

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


class TestDataFetchingTask(unittest.TestCase):
    def test_get_cities_weather(self):
        task = DataFetchingTask()
        task.get_cities_weather(cities=CITIES)
        self.assertIsNotNone(task.weather_info)


class TestDataCalculationTask(unittest.TestCase):
    def test_run_concurrent(self):
        task = DataCalculationTask(info={})
        task.run_concurrent(cities=CITIES.keys())
        self.assertEqual(len(task.weather_analytics), len(CITIES))


class TestDataAggregationTask(unittest.TestCase):
    def test_process_partly_data_and_merge(self):
        df_mock = [{
            "city": "FALLENANGELSCITY",
            "date": "2023-01-01",
            "avg_temp": -1,
            "n_hours_good_weather": 0,
        },
            {
                "city": "PARIS",
                "date": "2023-01-01",
                "avg_temp": -1,
                "n_hours_good_weather": 0}]
        task = DataAggregationTask(data=df_mock)


class TestDataAnalyzingTask(unittest.TestCase):
    def test_analyze_cities(self):
        mock_df = pd.read_csv(os.path.join("examples", "TEST.csv"), sep=',')
        data_analysis = DataAnalyzingTask(df=mock_df)
        best_city = data_analysis.analyze_cities()
        best_city = ",".join(best_city)
        self.assertEqual(best_city, 'CAIRO')



if __name__ == "__main__":
    unittest.main()
