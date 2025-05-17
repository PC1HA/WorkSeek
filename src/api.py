import requests
import time

# Константы API
API_URL = "https://api.hh.ru"


def get_companies(ids):
    """
    Получает информацию о компаниях по списку их id.
    """
    companies = []
    for company_id in ids:
        response = requests.get(f"{API_URL}/employers/{company_id}")
        if response.status_code == 200:
            companies.append(response.json())
        else:
            print(f"Ошибка получения компании {company_id}: {response.status_code}")
        time.sleep(0.2)  # чтобы не перегружать API
    return companies


def get_vacancies_for_company(company_id, per_page=20):
    """
    Получает вакансии для компании по её id.
    """
    vacancies = []
    page = 0
    while True:
        params = {
            'employer_id': company_id,
            'per_page': per_page,
            'page': page,
        }
        response = requests.get(f"{API_URL}/vacancies", params=params)
        if response.status_code != 200:
            print(f"Ошибка получения вакансий для компании {company_id}: {response.status_code}")
            break
        data = response.json()
        vacancies.extend(data['items'])
        if data['pages'] <= page + 1:
            break
        page += 1
        time.sleep(0.2)
    return vacancies


def search_companies_by_name(names):
    """
    Поиск компаний по названиям (используется поиск через API).
    Возвращает список компаний.
    """
    companies = []
    for name in names:
        params = {
            'text': name,
            'per_page': 1,
        }
        response = requests.get(f"{API_URL}/employers", params=params)
        if response.status_code == 200:
            items = response.json().get('items', [])
            if items:
                companies.append(items[0])
        else:
            print(f"Ошибка поиска компании {name}: {response.status_code}")
        time.sleep(0.2)
    return companies


def get_vacancy_details(vacancy):
    """
    Извлекает необходимые данные из вакансии.
    """
    salary_from = vacancy['salary']['from'] if vacancy['salary'] else None
    salary_to = vacancy['salary']['to'] if vacancy['salary'] else None
    salary_currency = vacancy['salary']['currency'] if vacancy['salary'] else None

    # Средняя зарплата (если есть диапазон)
    if salary_from and salary_to:
        avg_salary = (salary_from + salary_to) / 2
    elif salary_from:
        avg_salary = salary_from * 1.2  # предположительно выше минимальной зарплаты
    elif salary_to:
        avg_salary = salary_to * 0.8  # предположительно ниже максимальной зарплаты
    else:
        avg_salary = None

    return {
        'name': vacancy['name'],
        'url': vacancy['url'],
        'salary': avg_salary,
        'company_id': vacancy['employer']['id']
    }
