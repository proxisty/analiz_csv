analiz_csv
====
Установка
----
#### 1)Установить виртуальное окружение:
python -m venv venv

venv\Scripts\activate

git clone https://github.com/proxisty/analiz_csv.git

cd analiz_csv

pip install -r requirements.txt

#### 2)Создание базы данных. Если именяете параметры бд, тогда измените config_base в main.py. 
####Пример создания через psql:
create database dns;

create user dns WITH PASSWORD 'dns';

ALTER ROLE dns SET client_encoding TO 'utf8';

ALTER ROLE dns SET default_transaction_isolation TO 'read committed';

ALTER ROLE dns SET timezone TO 'Asia/Vladivostok';

GRANT ALL PRIVILEGES ON DATABASE dns TO dns;

grant pg_read_server_files to dns;  

#### 3)Переместите файлы t_branches.csv, t_cities.csv, t_products.csv, t_sales.csv в корень проекта, где main.py. Для изменения пути к файлам измените path_csv_files в main.py
#### 4)Запускаем исполняемый файл и ждем первого вывода в консоль в среднем после 2 минут:
python main.py

----
Демонстрация:
#### 1) "Аналитическая часть" #1 и #2 реализуется в методе complete_analytical_tasks. Реализована запросами к бд.
#### 2) "Аналитическая часть" #3 реализуется в методе complete_analytical_tasks3 при помощи pandas и matplotlib. В последствии последовательно выводятся графики (Для последующего выполнения программы гистограмму необходимо закрыть!):
![image](https://user-images.githubusercontent.com/42601425/177815753-3761acbf-0dc1-49d7-acc7-09b5f54bfd12.png)
![image](https://user-images.githubusercontent.com/42601425/177816221-60de0e36-e15f-4165-b1be-bad47d16d477.png)
#### 3) "Расчетная часть" #3 реализуется в методе execute_spreadsheet. В последствии в бд создается таблица quantile:
![image](https://user-images.githubusercontent.com/42601425/177816981-419de922-5057-4507-9114-931817dd4687.png)
#### 4) "Дополнительное задание, Расчетная часть" реализуется в методе create_and_export. Таблицы импортированы, основные индексы созданы. 
SELECT * FROM pg_indexes WHERE tablename in ('branches', 'cities', 'products', 'quantile', 'sales');

SELECT * FROM pg_tables p where p.tablename in ('branches', 'cities', 'products', 'quantile', 'sales');
#### 5) "Будет плюсом, если" 
-	скрипты будут фиксировать время выполнения; - реализовано при помощи декоратора timer
-	в коде будет присутствовать комментарии; - +
-	задачи будут решены с помощью Python; - +
-	задачи будут решены с использованием пакета pandas; - +
-	задачи будут решены с использованием sql-запросов к базе данных postgres. - +
