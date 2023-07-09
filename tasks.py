import concurrent.futures
from queue import Empty, Queue
from threading import Thread

import pandas as pd

from external.client import YandexWeatherAPI
from log_progress import logger


class DataFetchingTask:
    def __init__(self):
        self.queue = Queue()
        self.weather_info = {}
        self.workers = 5

    @staticmethod
    def get_weather(url) -> dict:
        return YandexWeatherAPI.get_forecasting(url)

    def worker(self):
        while True:
            task = self.queue.get()
            city, url = task
            try:
                weather_data = self.get_weather(url)
                self.weather_info[city] = weather_data
            except Exception as e:
                logger.error(f"Failed fetching data for city: {city}")
                logger.error(f"{str(e)}")
            finally:
                self.queue.task_done()

    def get_cities_weather(self, cities):
        for _ in range(self.workers):
            thread = Thread(target=self.worker)
            thread.daemon = True
            thread.start()

        for city, url in cities.items():
            self.queue.put((city, url))

        self.queue.join()

        # Clear the remaining items in the queue
        while True:
            try:
                self.queue.get(block=False)
            except Empty:
                break


class DataCalculationTask:
    def __init__(self, info: dict):
        self.info = info
        self.weather_analytics = {}

    def get_city_temp(self, city: str, forecast_hours=tuple(range(9, 20))) -> dict:
        result = {}
        try:
            city_data = self.info[city]
        except KeyError:
            logger.error(f"Failed forecasts data extraction for: {city}")
            return result

        try:
            forecasts = city_data["forecasts"]
        except KeyError:
            logger.error(f"Failed forecasts data extraction for: {city}")
            return result

        for forecast_ in forecasts:
            if len(forecast_["hours"]) < 24:
                continue
            date = forecast_["date"]
            result[date] = [
                {"condition": hourly_data["condition"],
                 "temp": hourly_data["temp"]}
                for hourly_data in forecast_["hours"]
                if int(hourly_data["hour"]) in forecast_hours
            ]
        return result

    @staticmethod
    def weather_conditions_calc(hours_data: list) -> int:
        good_conditions = ("partly-cloud", "clear", "cloudy", "overcast")
        return sum(1 for hourly_data in hours_data if hourly_data["condition"] in good_conditions)

    @staticmethod
    def avg_temp(hours_data: list) -> list[int, float]:
        return sum(hourly_data["temp"] for hourly_data in hours_data) / len(hours_data)

    def calc_weather_stats(self, city: str) -> dict:
        result = []
        try:
            city_data = self.get_city_temp(city)
        except KeyError:
            logger.error(f"Failed calcultaing temperature for: {city}")
            return {}

        for dt, forecast in city_data.items():
            n_hours_with_good_weather = self.weather_conditions_calc(hours_data=forecast)
            avg_temp = self.avg_temp(hours_data=forecast)
            weather_data = {
                "avg_temp": avg_temp,
                "n_hours_good_weather": n_hours_with_good_weather,
            }
            result.append({"date": dt, "weather_data": weather_data})

        return {city: result}

    def run_concurrent(self, cities: list):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(self.calc_weather_stats, city) for city in cities]

            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                self.weather_analytics.update(result)


class DataAggregationTask:
    def __init__(self, data: dict):
        self.data = data
        self.df = None

    @staticmethod
    def process_partly_data(partly_data: list) -> pd.DataFrame:
        results = []
        for row in partly_data:
            # row[0] - city, row[1] - temperature data
            for el in row[1]:
                results.append(
                    {"city": row[0],
                     "date": el['date'],
                     "avg_temp": el['weather_data']['avg_temp'],
                     "n_hours_good_weather": el['weather_data']['n_hours_good_weather'],
                     }
                )

        df = pd.DataFrame(results)
        return df

    @staticmethod
    def chunks(lst: list, n: int) -> list:
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def merge_results(self, workers: int = 5) -> pd.DataFrame:
        items = list(self.data.items())
        batch_size = (len(items) + workers - 1) // workers  # Adjust chunk size to ensure all items are processed
        batches = self.chunks(items, batch_size)

        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(self.process_partly_data, batches))

        merged_results = pd.concat(results, ignore_index=True).fillna("")
        merged_results = merged_results.groupby('city') \
            .agg({'avg_temp': 'mean', 'n_hours_good_weather': 'sum'}) \
            .reset_index()
        merged_results['rank_temp'] = merged_results.avg_temp.rank(ascending=True).astype(int)
        merged_results['rank_good_hours'] = merged_results.n_hours_good_weather.rank(ascending=True).astype(int)
        merged_results['cumulative_rank'] = merged_results['rank_temp'] + merged_results['rank_good_hours']

        self.df = merged_results
        return self.df
        #print(self.df)
        #self.df.to_csv('./examples/test2.csv', index=False, sep=';')

    def save_results(self):
        with pd.ExcelWriter('weather-stats.xlsx', mode='w') as writer:
            self.df.to_excel(writer)


class DataAnalyzingTask:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def analyze_cities(self) -> list:
        best_rank = self.df["cumulative_rank"].max()
        best_cities = self.df.loc[self.df["cumulative_rank"] == best_rank]
        best_cities = list(best_cities.city.values)
        return best_cities
