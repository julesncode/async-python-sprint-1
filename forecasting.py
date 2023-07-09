# import logging
# import threading
# import subprocess
# import multiprocessing


from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, get_url_by_city_name
from external.client import YandexWeatherAPI


def forecast_weather():
    """
    Анализ погодных условий по городам
    """

    #1: Получите информацию о погодных условиях для указанного списка городов
    cities_weather_data = {}
    for city_name, city_uri in CITIES.items():
        data_url = get_url_by_city_name(city_name)
        weather_data = YandexWeatherAPI.get_forecasting(data_url)
        cities_weather_data[city_name] = weather_data

    #2: Вычислите среднюю температуру и проанализируйте информацию об осадках за указанный период для всех городов
    weather_summary = {}
    for city_name, weather_data in cities_weather_data.items():
        temperatures = [hourly_data["temp"] for hourly_data in weather_data["hourly"]]
        average_temperature = sum(temperatures) / len(temperatures)

        precipitation = any(hourly_data.get("precipitation") for hourly_data in weather_data["hourly"])

        weather_summary[city_name] = {
            "average_temperature": average_temperature,
            "precipitation": precipitation,
        }

    # Print
    for city_name, summary in weather_summary.items():
        print(f"City: {city_name}")
        print(f"Average Temperature: {summary['average_temperature']}")
        print(f"Precipitation: {'Yes' if summary['precipitation'] else 'No'}")
        print()

if __name__ == "__main__":
    forecast_weather()
