import os
from src.api import get_companies, get_vacancies_for_company, search_companies_by_name, get_vacancy_details
from src.DBManager import DBManager

# Настройки подключения к базе данных - замените на свои параметры или используйте переменные окружения.
DB_NAME = "your_db"
DB_USER = "your_user"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = 5432


def main():
    # Создаем подключение и таблицы
    db_manager = DBManager(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)

    print("Создаю таблицы...")
    db_manager.create_tables()

    # Выбираем интересные компании по названиям или ID - здесь пример по названиям.
    company_names = [
        "Google",
        "Yandex",
        "Microsoft",
        "Apple",
        "Amazon",
        "Facebook",
        "Tesla",
        "Samsung",
        "IBM",
        "Oracle"
    ]

    print("Ищу компании...")
    companies_info = search_companies_by_name(company_names)

    print("Добавляю работодателей в базу...")
    # Формируем список работодателей для вставки в БД
    employers_data = []
    for comp in companies_info:
        employers_data.append({
            'employer_id': comp['id'],
            'name': comp.get('name', ''),
            'url': comp.get('url', '')
        })
    db_manager.insert_employers(employers_data)

    print("Получаю вакансии для компаний...")
    all_vacancies = []
    for comp in companies_info:
        vacancies_raw = get_vacancies_for_company(comp['id'])
        for vac in vacancies_raw:
            vac_data = get_vacancy_details(vac)
            all_vacancies.append(vac_data)

    print("Добавляю вакансии в базу...")
    db_manager.insert_vacancies(all_vacancies)

    # Взаимодействие с пользователем: пример вызова методов класса DBManager

    print("\n=== Статистика ===")

    print("\nКомпании и количество вакансий:")
    for row in db_manager.get_companies_and_vacancies_count():
        print(f"Компания: {row[0]}, Вакансий: {row[1]}")

    print("\nВсе вакансии:")
    for row in db_manager.get_all_vacancies():
        print(f"Компания: {row[0]}, Вакансия: {row[1]}, Зарплата: {row[2]}, Ссылка: {row[3]}")

    avg_salary = db_manager.get_avg_salary()
    print(f"\nСредняя зарплата по вакансиям: {avg_salary:.2f}" if avg_salary else "\nНет данных о зарплате.")

    print("\nВакансии с зарплатой выше средней:")
    for row in DBManager
