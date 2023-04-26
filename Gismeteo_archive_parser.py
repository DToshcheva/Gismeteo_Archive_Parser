# Импорт библиотек
import multiprocessing
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date

# Потребуется для requests.get
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/112.0.0.0 Safari/537.36'}


# Функция собирает в список словари с информацией для каждого города о его районе, стране (наименование и id)
def get_cities(url_cities):
    city_list = []
    cities = requests.get(url_cities, headers=headers)
    cities_item = [
        {'city_id': i.get('id'),
         'city_name': i.get('n'),
         'district_id': i.get('district_id'),
         'district_name': i.get('district_name'),
         'country_id': i.get('country_id'),
         'country_name': i.get('country_name')
         }
        for i in BeautifulSoup(cities.text, 'xml').find_all('item')
    ]
    city_list.append(cities_item)
    return city_list


def get_info(countries_id):
    # Собираем список районов для каждой страны

    district_list = []
    url_districts = f"https://www.gismeteo.ru/inform-service/63466572668a39754a9ddf4c8b3437b0/districts/?country={countries_id}&fr=sel"
    d = requests.get(url_districts, headers=headers)
    districts_id = [i.get('id') for i in BeautifulSoup(d.text, 'xml').find_all('item')]
    district_list.extend(districts_id)

    # Создаем список ссылок для requests.get для обращения к конкретному городу
    # Вилка на случай, если у города нет районов
    if not district_list:
        url_cities = [
            f"https://www.gismeteo.ru/inform-service/63466572668a39754a9ddf4c8b3437b0/cities/?country={countries_id}&fr=sel"]
    else:
        url_cities = [
            f"https://www.gismeteo.ru/inform-service/63466572668a39754a9ddf4c8b3437b0/cities/?district={district}&fr=sel"
            for district in district_list]

    # Отправляем запрос по собранным ссылкам через get_cities
    result = []
    for url in url_cities:
        result.extend(get_cities(url))
    city_list = [item for sublist in result for item in sublist]

    # Получаем список словарей с информацией для каждого города о его районе, стране (наименование и id)
    return city_list


def get_temperature(city_list):
    # Определяем период, за который собираем информацию о погоде
    years = list(range(2022, 2024))
    months = list(range(12, 13))

    df = []
    for city in city_list:
        city_id = city['city_id']
        for year in years:
            for month in months:
                if date(year, month,
                        1) < date.today():  # Невозможно отправить запрос если дата превышает текущую (сравнение месяцев)
                    url = f'https://www.gismeteo.ru/diary/{city_id}/{year}/{month}'
                    r = requests.get(url, headers=headers)
                    soup = BeautifulSoup(r.text, 'html.parser')
                    data = soup.find('div', {'id': 'data_block'})

                    # Игнорировать ошибки в случае отсутствия ответа
                    try:
                        days = [i.text for i in data.find_all('td', {'class': 'first'})]
                        temperature = [i.text for i in
                                       data.find_all('td', {'class': ['first_in_group positive', 'first_in_group']})]
                    except:
                        continue

                    # Нарезка, чтобы разделить дневную и ночную температуру
                    temperature_day = temperature[::2]
                    temperature_night = temperature[1::2]

                    # Формируем итоговый датафрейм из полученных данных (город-год-месяц)
                    res = pd.DataFrame({'day': pd.Series(days),
                                        'temperature_day': pd.Series(temperature_day),
                                        'temperature_night': pd.Series(temperature_night),
                                        'city': city['city_name'],
                                        'district': city['district_name'],
                                        'country': city['country_name'],
                                        'year': year,
                                        'month': month
                                        })
                    df.append(res)
    concatenated_df = pd.concat(df)
    return concatenated_df


# Разбиение списка на подсписки
def chunks(xs, n):
    n = max(1, n)
    return (xs[i:i + n] for i in range(0, len(xs), n))


if __name__ == '__main__':
    # requests.get собирает все доступные страны
    url_countries = "https://www.gismeteo.ru/inform-service/63466572668a39754a9ddf4c8b3437b0/countries/?fr=sel"
    c = requests.get(url_countries, headers=headers)
    countries_id = [i.get('id') for i in BeautifulSoup(c.text, 'xml').find_all('item')]

    with multiprocessing.Pool(60) as p:
        r = p.map(get_info, countries_id)
        r = [item for sublist in r for item in sublist]
        m = p.map(get_temperature, chunks(r, 60))
        temperature = pd.concat(m)
        temperature.to_csv('Multiprocessing temp result.csv', sep=';')
