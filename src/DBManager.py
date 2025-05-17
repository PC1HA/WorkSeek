import psycopg2
from psycopg2 import sql


class DBManager:
    def __init__(self, db_name, user, password, host='localhost', port=5432):
        self.connection_params = {
            'dbname': db_name,
            'user': user,
            'password': password,
            'host': host,
            'port': port,
        }
        self.conn = psycopg2.connect(**self.connection_params)
        self.cur = self.conn.cursor()

    def create_tables(self):
        """
        Создает таблицы employers и vacancies.
        """
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS employers (
                id SERIAL PRIMARY KEY,
                employer_id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(255),
                url TEXT
            );
        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                vacancy_name VARCHAR(255),
                url TEXT,
                salary NUMERIC,
                employer_id INTEGER REFERENCES employers(id)
            );
        """)

        self.conn.commit()

    def insert_employers(self, employers):
        """
        Вставляет список работодателей.

        :param employers: список dict с ключами: employer_id, name, url
        """
        for emp in employers:
            self.cur.execute("""
                INSERT INTO employers (employer_id, name, url)
                VALUES (%s, %s, %s)
                ON CONFLICT (employer_id) DO NOTHING;
            """, (emp['employer_id'], emp['name'], emp['url']))

        self.conn.commit()

    def insert_vacancies(self, vacancies):
        """
        Вставляет список вакансий.

        :param vacancies: список dict с ключами: vacancy_name, url, salary, employer_id (внутри таблицы employers)

       """

    for vac in vacancies:
        # Получаем внутренний id работодателя по employer_id из таблицы employers
        self.cur.execute("""
               SELECT id FROM employers WHERE employer_id=%s;
           """, (vac['company_id'],))
        employer_row = self.cur.fetchone()
        if employer_row:
            employer_db_id = employer_row[0]
            self.cur.execute("""
                   INSERT INTO vacancies (vacancy_name, url, salary, employer_id)
                   VALUES (%s, %s, %s, %s);
               """, (vac['name'], vac['url'], vac['salary'], employer_db_id))
    self.conn.commit()


def get_companies_and_vacancies_count(self):
    """
    Возвращает список компаний и количество вакансий у каждой.
    """
    self.cur.execute("""
           SELECT e.name, COUNT(v.id) as vacancies_count 
           FROM employers e 
           LEFT JOIN vacancies v ON e.id=v.employer_id 
           GROUP BY e.name;
       """)
    return self.cur.fetchall()


def get_all_vacancies(self):
    """
    Возвращает все вакансии с названием компании.
    """
    self.cur.execute("""
           SELECT e.name as company_name, v.vacancy_name, v.salary, v.url 
           FROM vacancies v 
           JOIN employers e ON v.employer_id=e.id;
       """)
    return self.cur.fetchall()


def get_avg_salary(self):
    """
    Возвращает среднюю зарплату по вакансиям.
    """
    self.cur.execute("""
           SELECT AVG(salary) FROM vacancies WHERE salary IS NOT NULL;
       """)
    result = self.cur.fetchone()
    return result[0] if result else None


def get_vacancies_with_higher_salary(self):
    """
    Возвращает вакансии с зарплатой выше средней.
    """
    avg_salary = self.get_avg_salary()
    if not avg_salary:
        return []
    self.cur.execute("""
           SELECT e.name as company_name, v.vacancy_name, v.salary, v.url 
           FROM vacancies v 
           JOIN employers e ON v.employer_id=e.id 
           WHERE v.salary > %s;
       """, (avg_salary,))
    return self.cur.fetchall()


def get_vacancies_with_keyword(self, keyword):
    """
    Возвращает вакансии в названии которых содержится слово keyword.
    """
    pattern = f"%{keyword}%"
    self.cur.execute("""
           SELECT e.name as company_name, v.vacancy_name, v.salary, v.url 
           FROM vacancies v 
           JOIN employers e ON v.employer_id=e.id 
           WHERE v.vacancy_name ILIKE %s;
       """, (pattern,))
    return self.cur.fetchall()


def close(self):
    self.cur.close()
    self.conn.close()
