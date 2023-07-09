import unittest
from unittest.mock import patch
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
    def setUp(self):
        self.data = {
            "City1": [
                {
                    "date": "2023-01-01",
                    "weather_data": {"avg_temp": 10.5, "n_hours_good_weather": 8},
                },
                {
                    "date": "2023-01-02",
                    "weather_data": {"avg_temp": 8.2, "n_hours_good_weather": 7},
                },
            ],
            "City2": [
                {
                    "date": "2023-03-01",
                    "weather_data": {"avg_temp": 12.1, "n_hours_good_weather": 9},
                },
                {
                    "date": "2023-01-02",
                    "weather_data": {"avg_temp": 9.8, "n_hours_good_weather": 6},
                },
            ],
        }

    def test_process_partly_data(self):
        task = DataAggregationTask(data={})
        partly_data = [
            ("City1", self.data["City1"]),
            ("City2", self.data["City2"]),
        ]
        expected_df = pd.DataFrame(
            [
                {"city": "City1", "date": "2023-01-01", "avg_temp": 10.5, "n_hours_good_weather": 8},
                {"city": "City1", "date": "2023-01-02", "avg_temp": 8.2, "n_hours_good_weather": 7},
                {"city": "City2", "date": "2023-03-01", "avg_temp": 12.1, "n_hours_good_weather": 9},
                {"city": "City2", "date": "2023-01-02", "avg_temp": 9.8, "n_hours_good_weather": 6},
            ]
        )

        result_df = task.process_partly_data(partly_data)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_chunks(self):
        task = DataAggregationTask(data={})
        lst = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        n = 3
        expected_chunks = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]

        result_chunks = list(task.chunks(lst, n))
        self.assertEqual(result_chunks, expected_chunks)

    def test_merge_results(self):
        mock_df = pd.read_csv(os.path.join("examples", "TEST2.csv"), sep=';').to_json()
        task = DataAggregationTask(data=self.data)
        results = task.merge_results(workers=2).to_json()
        self.assertEqual(results, mock_df)


if __name__ == "__main__":
    unittest.main()


class TestDataAnalyzingTask(unittest.TestCase):
    def test_analyze_cities(self):
        mock_df = pd.read_csv(os.path.join("examples", "TEST.csv"), sep=',')
        data_analysis = DataAnalyzingTask(df=mock_df)
        best_city = data_analysis.analyze_cities()
        best_city = ",".join(best_city)
        self.assertEqual(best_city, 'CAIRO')


if __name__ == "__main__":
    unittest.main()
