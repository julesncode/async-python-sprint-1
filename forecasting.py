from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """

    # Получите информацию о погодных условиях для указанного списка городов
    cities_weather_data = DataFetchingTask()
    cities_weather_data.get_cities_weather(cities=CITIES)
    cities_weather = cities_weather_data.weather_info

    # Вычислите среднюю температуру и проанализируйте информацию об осадках за указанный период для всех городов
    calculation_task = DataCalculationTask(info=cities_weather)
    calculation_task.run_concurrent(cities=CITIES.keys())
    weather_analytics = calculation_task.weather_analytics

    # Объедините полученные данные и сохраните результат в текстовом файле
    aggregated_data = DataAggregationTask(data=weather_analytics)
    aggregated_data.merge_results()
    aggregated_data.save_results()

    # Проанализируйте результат и сделайте вывод, какой из городов наиболее благоприятен для поездки.
    data_analysis = DataAnalyzingTask(df=aggregated_data.df)
    best_city = data_analysis.analyze_cities()
    best_city = ",".join(best_city)
    print(f"Best city(-es): {best_city}")


if __name__ == "__main__":
    forecast_weather()
