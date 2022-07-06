import psycopg2
import pathlib
import time

# конфигурация коннекта к бд
config_base = {'database': 'dns', 'user': 'dns', 'password': 'dns', 'host': '127.0.0.1', 'port': '5432'}
# Путь к файлам для импорта. Файлы должны быть в корне проекта.
dir_work = pathlib.Path.cwd()
path_csv_files = {'branches': f"{pathlib.Path(dir_work, 't_branches.csv')}",
                  'products': f"{pathlib.Path(dir_work, 't_products.csv')}",
                  'cities': f"{pathlib.Path(dir_work, 't_cities.csv')}",
                  'sales': f"{pathlib.Path(dir_work, 't_sales.csv')}"}


def create_and_export(config_base, path_csv_files):
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
                                         name             varchar(50) NOT NULL,
                                         citi_citi_link   varchar(36) REFERENCES cities (citi_link),
									     short_name       varchar(50) DEFAULT NULL,
									     region           varchar(50) DEFAULT NULL);
	CREATE TABLE IF NOT EXISTS products (prod_id          smallint UNIQUE,
                                         prod_link        varchar(36) PRIMARY KEY,
                                         name             varchar);                                    
    CREATE TABLE IF NOT EXISTS sales    (sals_id          integer UNIQUE,
                                      	exec_time         timestamp without time zone,
                                      	brnc_brnc_link    varchar(36) REFERENCES branches (brnc_link),
								 	    prod_prod_link    varchar(36) REFERENCES products (prod_link),
								      	quantity          decimal,
								        price             decimal);
								      '''
    cursor.execute(sql_create_tables)
    #  REFERENCES cities (citi_link)

    # Импортируем cities
    sql_import_cities = f"COPY cities(citi_id,citi_link,name) FROM '{path_csv_files['cities']}' DELIMITER ',' CSV HEADER;"
    cursor.execute(sql_import_cities)
    # Импортируем branches
    sql_import_branches = f"COPY branches(brnc_id,brnc_link,name,citi_citi_link, short_name, region) FROM '{path_csv_files['branches']}' DELIMITER ',' CSV HEADER;"
    cursor.execute(sql_import_branches)
    # Импортируем products
    sql_import_products = f"COPY products(prod_id,prod_link,name) FROM '{path_csv_files['products']}' DELIMITER ',' CSV HEADER;"
    cursor.execute(sql_import_products)
    # Импортируем sales
    sql_import_sales = f"COPY sales(sals_id,exec_time,brnc_brnc_link, prod_prod_link, quantity, price) FROM '{path_csv_files['sales']}' DELIMITER ',' CSV HEADER;"
    cursor.execute(sql_import_sales)
    #
    # sql3 = '''select * from branches;'''
    # cursor.execute(sql3)
    # for i in cursor.fetchall():
    #     print(i)

    conn.commit()

    conn.close()


if __name__ == "__main__":
    # Импорт за 55.46
    start_time = time.time()
    create_and_export(config_base, path_csv_files)
    print(f"Время потраченное на загрузку csv: {(time.time() - start_time)}")

# elapsed_time = timeit.timeit(create_and_export(config_base), number=100) / 100
# print(elapsed_time)
