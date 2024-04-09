"""
Данные из ЦБР в Greenplum
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

import csv
import xml.etree.ElementTree as ET # Импортировали из библиотеки xml элемент tree и назвали его ET

from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 't-kajyrmagambetov',
    'poke_interval': 600
}

FILENAME = 't-kajyrmagambetov_cbr'
GP_TABLENAME = 't_kajyrmagambetov_cbr'
# url='https://www.cbr.ru/scripts/XML_daily.asp?date_req=13/11/2023'

dag = DAG("t-kajyrmagambetov_load_cbr",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['t-kajyrmagambetov']
          )

# Use execution date to format the URL
def get_cbr_url(execution_date):
    formatted_date = execution_date.strftime('%d/%m/%Y')
    return f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={formatted_date}'


# BashOperator to check for the file's existence and delete it if necessary
export_cbr_xml = BashOperator(
    task_id='export_cbr_xml',
    bash_command='rm -f /tmp/{filename}.xml; curl "$(get_cbr_url {{ execution_date }})" | iconv -f Windows-1251 -t UTF-8 > /tmp/{filename}.xml'.format(filename=FILENAME),
    dag=dag
)

# export_cbr_xml = BashOperator(
#     task_id='export_cbr_xml',
#     bash_command='curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/{filename}.xml'.format(url=url,filename=filename),
#     dag=dag
# )

def xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8") # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/file_name.xml', parser=parser)
    tree = ET.parse('/tmp/{filename}.xml'.format(filename=FILENAME), parser=parser)
    root = tree.getroot() # Корень нашего файла

    with open('/tmp/{filename}.csv'.format(filename=FILENAME), 'w') as csv_file: # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует:Valute, NumCode и т.д.
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for Valute in root.findall('Valute'):
            NumCode = Valute.find('NumCode').text
            CharCode = Valute.find('CharCode').text
            Nominal = Valute.find('Nominal').text
            Name = Valute.find('Name').text
            Value = Valute.find('Value').text
            writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                            [Name] + [Value.replace(',', '.')]) # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того, чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит, что это переход на новое значение
            logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                        [Name] + [Value.replace(',', '.')]) # Логируем все в log airflow, чтобы посмотреть  все ли хорошо

xml_to_csv = PythonOperator(
    task_id='xml_to_csv',
    python_callable=xml_to_csv_func,
    dag=dag
)

def load_csv_to_gp_func():
    pg_hook = PostgresHook('conn_greenplum_write') # Создаем hook, записываем наш гринплан
    # TODO: Мёрдж или очищение предыдущего батча
    pg_hook.copy_expert("COPY {gp_tablename} FROM STDIN DELIMITER ','".format(gp_tablename=GP_TABLENAME),'/tmp/{filename}.csv'.format(filename=FILENAME))

load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

export_cbr_xml >> xml_to_csv >> load_csv_to_gp
