import psycopg2
import pathlib
import time
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import numpy as np

# конфигурация коннекта к бд
config_base = {'database': 'dns', 'user': 'dns', 'password': 'dns', 'host': '127.0.0.1', 'port': '5432'}
# Путь к файлам для импорта. Файлы должны быть в корне проекта.
dir_work = pathlib.Path.cwd()
path_csv_files = {'branches': f"{pathlib.Path(dir_work, 't_branches.csv')}",
                  'products': f"{pathlib.Path(dir_work, 't_products.csv')}",
                  'cities': f"{pathlib.Path(dir_work, 't_cities.csv')}",
                  'sales': f"{pathlib.Path(dir_work, 't_sales.csv')}"}


def timer(f):
    """Декоратор для определения времени выполнения метода"""
    def tmp(*args, **kwargs):
        start_time = time.monotonic()
        res = f(*args, **kwargs)
        print(f"Время выполнения функции: {time.monotonic() - start_time}")
        return res
    return tmp


@timer
def create_and_export(config_base, path_csv_files):
    """Метод импорта csv файлов"""
    conn = psycopg2.connect(database=config_base['database'],
                            user=config_base['user'],
                            password=config_base['password'],
                            host=config_base['host'],
                            port=config_base['port'])
    conn.autocommit = True
    cursor = conn.cursor()

    sql_create_tables = '''
    drop table if exists branches, products, cities, sales;
    CREATE TABLE IF NOT EXISTS cities   (citi_id          smallint UNIQUE ,
                                        citi_link         varchar(36) PRIMARY KEY,
                                        name              varchar); 
    CREATE TABLE IF NOT EXISTS branches (brnc_id          smallint UNIQUE,
                                         brnc_link        varchar(36) PRIMARY KEY,
                                         name             varchar(50),
                                         citi_citi_link   varchar(36), -- REFERENCES cities (citi_link)
									     short_name       varchar(50) DEFAULT NULL,
									     region           varchar(50) DEFAULT NULL);
	CREATE TABLE IF NOT EXISTS products (prod_id          smallint UNIQUE,
                                         prod_link        varchar(36) PRIMARY KEY,
                                         name             varchar NOT NULL);                                    
    CREATE TABLE IF NOT EXISTS sales    (sals_id          integer UNIQUE,
                                      	exec_time         timestamp without time zone,
                                      	brnc_brnc_link    varchar(36), -- REFERENCES branches (brnc_link)
								 	    prod_prod_link    varchar(36), -- REFERENCES products (prod_link)
								      	quantity          NUMERIC(4,0),
								        price             decimal);
								      '''
    cursor.execute(sql_create_tables)

    # Импортируем cities
    sql_import_cities = f"COPY cities(citi_id,citi_link,name) " \
                        f"FROM '{path_csv_files['cities']}' DELIMITER ',' CSV HEADER;"
    cursor.execute(sql_import_cities)
    # Импортируем branches
    sql_import_branches = f"COPY branches(brnc_id,brnc_link,name,citi_citi_link, short_name, region) " \
                          f"FROM '{path_csv_files['branches']}' DELIMITER ',' CSV HEADER;"
    cursor.execute(sql_import_branches)
    # Импортируем products
    sql_import_products = f"COPY products(prod_id,prod_link,name) " \
                          f"FROM '{path_csv_files['products']}' DELIMITER ',' CSV HEADER;"
    cursor.execute(sql_import_products)
    # Импортируем sales
    sql_import_sales = f"COPY sales(sals_id,exec_time,brnc_brnc_link, prod_prod_link, quantity, price) " \
                       f"FROM '{path_csv_files['sales']}' DELIMITER ',' CSV HEADER;"
    cursor.execute(sql_import_sales)
    # Создаем индексы для таблицы sales
    create_index = '''create INDEX brnc_brnc_link_key on sales (brnc_brnc_link);
                      create INDEX prod_prod_link_key on sales (prod_prod_link);'''
    cursor.execute(create_index)
    print("Задание create_index:")

    conn.commit()
    conn.close()


@timer
def complete_analytical_tasks(config_base):
    """ Выполнение аналитической части заданий:
    1. Требуется рассчитать и вывести название и количество в порядке убывания:
        1)	десять первых магазинов по количеству продаж;
        2)	десять первых складов по количеству продаж;
        3)	десять самых продаваемых товаров по складам;
        4)	десять самых продаваемых товаров по магазинам;
        5)	десять городов, в которых больше всего продавалось товаров.
    2. Требуется рассчитать и вывести в какие часы и в какой день недели происходит максимальное количество продаж.
    3. Дополнительное задание: вывести на графике количество продаж в каждом часе, и количество продаж по дням недели
    """
    conn = psycopg2.connect(database=config_base['database'],
                            user=config_base['user'],
                            password=config_base['password'],
                            host=config_base['host'],
                            port=config_base['port'])
    cursor = conn.cursor()
    # Задание 1.1
    sql1_1 = '''select b.short_name, sum(s.quantity) as count_sale from branches b, sales s
                where 1=1 and b.brnc_link = s.brnc_brnc_link
                group by b.short_name
                order by count_sale desc
                LIMIT 10;'''
    cursor.execute(sql1_1)
    print("Задание #1.1:")
    for i in cursor.fetchall():
        print(f"Магазин '{i[0]}' имеет {i[1]} продаж.")

    # Задание 1.2
    sql1_2 = '''select b.name, sum(s.quantity) as count_sale from sales s, branches b
                where 1=1 and s.brnc_brnc_link = b.brnc_link
                group by b.name
                order by count_sale desc
                LIMIT 10;'''
    cursor.execute(sql1_2)
    print("Задание #1.2:")
    for i in cursor.fetchall():
        print(f"Склад '{i[0]}' имеет {i[1]} продаж.")
    # Задание 1.3
    sql1_3 = '''select s.brnc_brnc_link, s.prod_prod_link, sum(s.quantity) as count_sale from sales s
                group by s.brnc_brnc_link, s.prod_prod_link
                order by count_sale desc
                LIMIT 10;'''
    cursor.execute(sql1_3)
    print("Задание #1.3:")
    for i in cursor.fetchall():
        print(f"В складе '{i[0]}' товар {i[1]} продался {i[2]} раз")
    # Задание 1.4
    sql1_4 = '''select b.short_name, s.prod_prod_link, sum(s.quantity) as count_sale from branches b, sales s
                where 1=1 and b.brnc_link = s.brnc_brnc_link
                group by b.short_name, s.prod_prod_link
                order by count_sale desc
                LIMIT 10;'''
    cursor.execute(sql1_4)
    print("Задание #1.4:")
    for i in cursor.fetchall():
        print(f"В магазине '{i[0]}' товар {i[1]} продался {i[2]} раз.")
    # Задание 1.5
    sql1_5 = '''select b.citi_citi_link, sum(s.quantity) as count_sale from branches b, sales s, cities c
                where 1=1 and b.brnc_link = s.brnc_brnc_link and c.citi_link = b.citi_citi_link
                group by b.citi_citi_link
                order by count_sale desc
                LIMIT 10;'''
    cursor.execute(sql1_5)
    print("Задание #1.5:")
    for i in cursor.fetchall():
        print(f"В городе '{i[0]}' товаров продалось {i[1]} раз")
    # Задание #2
    sql2 = '''with for_dd as (
                select extract(HOUR from s.exec_time) as dd, sum(s.quantity) as sum_dd
                from sales s
                group by dd
                order by sum_dd desc
                LIMIT 1
            ), for_hh as (
                select extract(DOW from s1.exec_time) as dd1, sum(s1.quantity) as sum_dd1
                from sales s1
                group by dd1
                order by sum_dd1 desc
                LIMIT 1
            )
            select * from for_hh, for_dd;'''
    cursor.execute(sql2)
    print("Задание #2:")
    for i in cursor.fetchall():
        print(f"В '{i[0]}' день недели произошло максимальное количество продаж: {i[1]} продаж"
              f"В '{i[2]}' часов происходило максимальное количество продаж: {i[3]} продаж")

    conn.close()


def complete_analytical_tasks3(config_base):
    """3 Дополнительное задание: вывести на графике количество продаж в каждом часе,
    и количество продаж по дням недели"""
    engine = create_engine(
        f"postgresql+psycopg2://{config_base['user']}:{config_base['password']}@{config_base['host']}:5432/{config_base['database']}")
    sql = '''select extract(HOUR from s.exec_time) as hours, sum(s.quantity) as quantity
            from sales s
            group by hours
            order by hours desc;'''
    df = pd.read_sql(sql, engine)
    df.plot(kind='bar', x="hours", y="quantity",
            title="Количество продаж в каждом часе", ylabel="Количество")
    plt.ticklabel_format(style='plain', axis='y')
    plt.show()

    sql1 = '''  select
                extract(DOW from s1.exec_time) as number_day, 
                case extract(DOW from s1.exec_time) 
                    when 0 THEN 'Вскр'
                    when 1 THEN 'Пон'
                    when 2 THEN 'Втор'
                    when 3 THEN 'Сред'
                    when 4 THEN 'Четв'
                    when 5 THEN 'Пятн'
                    when 6 THEN 'Суббо'
                END as day_on_the_week,
                sum(s1.quantity) as quantity
                from sales s1
                group by number_day
                order by number_day desc;'''
    df1 = pd.read_sql(sql1, engine)
    df1.plot(kind='bar', x="day_on_the_week", y="quantity",
             title="Количество продаж по дням", ylabel="Количество")
    plt.ticklabel_format(style='plain', axis='y')
    plt.show()


@timer
def execute_spreadsheet(config_base):
    """Расчетная часть заданий.
    Рассчитывается товары по классам относительно количества продаж относительно квантилям [0, .3, .9, 1]
    Создается таблица quantile в бд"""
    # Создаем DF на основе t_sales.csv
    col_names_sales = ['sals_id', 'exec_time', 'brnc_brnc_link', 'prod_prod_link', 'quantity', 'price']
    t_sales = pd.read_csv('t_sales.csv',
                          names=col_names_sales,  # колонки согласно col_names
                          skiprows=[0],  # пропускаем первую страницу
                          index_col='sals_id')  # , delimiter=','  Индекс по колонке id
    group_by_quantity_1 = t_sales.groupby('prod_prod_link', as_index=False).aggregate({'quantity': 'sum'}).sort_values(
        'quantity', ascending=False)
    # Делим df на три класса
    group_by_quantity_1['quantile'] = pd.qcut(group_by_quantity_1['quantity'], q=[0, .3, .9, 1],
                                              labels=['min', 'average', 'max'])
    # удаляем поле quantity
    group_by_quantity_1.drop(columns=['quantity'], axis=1, inplace=True)
    engine = create_engine(
        f"postgresql+psycopg2://{config_base['user']}:{config_base['password']}@{config_base['host']}:5432/{config_base['database']}")
    # импортируем df в бд
    group_by_quantity_1.to_sql('quantile', con=engine, if_exists='replace')


if __name__ == "__main__":
    # Создаем таблицы, индексы. Импортируем csv файлы в бд
    create_and_export(config_base, path_csv_files)
    # Выполнение аналитической части заданий
    complete_analytical_tasks(config_base)
    # Выполнение дополнительного задания аналитической части #3
    complete_analytical_tasks3(config_base)
    # Выполнение расчетной части задания
    execute_spreadsheet(config_base)
