# Импорт библиотек
import multiprocessing
from typing import List
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date


# Определяем функции

# Преобразование ввода пользовательских годов и месяцев в список
def str_to_lst(string):
    lst = []
    for i in ''.join(string.split(' ')).split(','):
        if '-' in i:
            first = int(i[:i.find('-')])
            last = int(i[i.find('-') + 1:]) + 1
            interval = list(range(first, last))
            lst.extend(interval)
        else:
            i = int(i)
            lst.append(i)
    return sorted(lst)


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


def get_info(countries_id: List[int]):
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


def build_params(cities_id, years, months, city_list):
    params = []
    for year in years:
        for month in months:
            for id in cities_id:
                params.append({
                    "month": month,
                    "year": year,
                    'city_id': id,
                    'city_name': find(city_list, 'city_id', id)['city_name'],
                    'district_name': find(city_list, 'city_id', id)['district_name']
                })

    return params


def get_temperature(params):
    year = params['year']
    month = params['month']
    id = params['city_id']
    name = params['city_name']
    district = params['district_name']
    df = []
    retry_list = []
    # Невозможно отправить запрос если дата превышает текущую (сравнение месяцев)
    if date(year, month, 1) < date.today():
        url = f'https://www.gismeteo.ru/diary/{id}/{year}/{month}'
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')
        data = soup.find('div', {'id': 'data_block'})

        # Игнорировать ошибки в случае отсутствия ответа
        try:
            days = [i.text for i in data.find_all('td', {'class': 'first'})]
            temperature = [i.text for i in
                           data.find_all('td', {'class': ['first_in_group positive', 'first_in_group']})]
        except:
            retry_list.append(url)

        # Нарезка, чтобы разделить дневную и ночную температуру
        temperature_day = temperature[::2]
        temperature_night = temperature[1::2]

        # Формируем итоговый датафрейм из полученных данных (город-год-месяц)
        res = pd.DataFrame({'day': pd.Series(days),
                            'temperature_day': pd.Series(temperature_day),
                            'temperature_night': pd.Series(temperature_night),
                            'city_id': id,
                            'city_name': name,
                            'district_name': district,
                            'year': year,
                            'month': month
                            })

        df.append(res)

    concatenated_df = pd.concat(df)
    return concatenated_df


def get_countries_info(country):
    url_countries = "https://www.gismeteo.ru/inform-service/63466572668a39754a9ddf4c8b3437b0/countries/?fr=sel"
    c = requests.get(url_countries, headers=headers)
    countries_id = []
    for i in BeautifulSoup(c.text, 'xml').find_all('item'):
        if i.get('n').lower() == country[0]:
            countries_id.append(i.get('id'))
    return countries_id


def find(input_list, key, value):
    for i in input_list:
        if i[key] == value:
            return i

    return None


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/112.0.0.0 Safari/537.36'}
if __name__ == '__main__':
    country_question = 'По какой стране будет поиск?\nПример ввода: Албания\n'
    city_question = 'По какому городу будет поиск?\nПример ввода: Белгород, Москва, Казань\n'
    year_question = 'За какой год?\nПример ввода : 2013, 2018, 2020-2022\n'
    month_question = 'За какой месяц?\nПример ввода: 1, 3, 10-12\n'

    country = input(f'{country_question}')
    cities = input(f'{city_question}')
    years = input(f'{year_question}')
    months = input(f'{month_question}')

    print(f'Сбор информации о погоде по следующим параметрам:\n'
          f'Страна: {country}\n'
          f'Города: {cities}\n'
          f'Годы: {years}\n'
          f'Месяцы: {months}\n')

    # преобразование ввода пользователей в удобный формат
    country = [country.lower()]
    cities = ''.join(cities.split(' ')).lower().split(',')
    years = str_to_lst(years)
    months = str_to_lst(months)

    # Поиск id страны из ввода пользователя

    with multiprocessing.Pool(60) as p:

        countries_id = get_countries_info(country)
        # Собираем список словарей для городов
        city_list = p.map(get_info, countries_id)
        city_list = [item for sublist in city_list for item in sublist]

        # Поиск id города из ввода пользователя (пробег по всему списку городов страны и сравнение с вводом пользователя)
        cities_id = []
        for city in city_list:
            if city['city_name'].lower() in cities:
                cities_id.append(city['city_id'])

        params = build_params(cities_id, years, months, city_list)
        temperature = p.map(get_temperature, params)
        result = pd.concat(temperature)

        print('Под каким именем сохранить файл?')
        file_name = input()
        result.to_csv(f'{file_name}.csv', sep=';')
