import mysql.connector
from mysql.connector import Error
import time
import os
import traceback
import concurrent.futures
import pandas as pd

def read_mysql_info_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf8') as file:
            lines = file.readlines()
            # print(lines)

            # ��ʼ���ֵ�
            variables = {}

            for line in lines:
                # ʹ�� split ����ȥ������������ȡֵ
                try:
                    var_name, var_value = map(str.strip, line.split('='))
                    variables[var_name] = var_value.strip('"')
                except Exception as e:
                    print(f"������ʱ����: {e}")

            return variables.get('host'), variables.get('user'), variables.get('password'), variables.get(
                'database'), variables.get('table'), variables.get('output_file')

    except Exception as e:
        print(f"��ȡ�ļ�����: {e}")
        return None


def export_mysql_data_chunk(host, user, password, database, table, header_info, rows, output_file):
    df = pd.DataFrame(rows, columns=header_info)
    with open(output_file, 'a', encoding='utf-8') as file:
        df.to_csv(file, index=False, header=False)

def export_mysql_data_batch_threaded(host, user, password, database, table, output_file, max_rows_per_sheet=1000000, num_threads=4):
    start_time = time.time()
    total_rows_exported = 0

    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print("�ɹ����ӵ����ݿ�")

            cursor = connection.cursor()

            cursor.execute(f"DESCRIBE {table}")
            header_info = [column[0] for column in cursor.fetchall()]

            query = f"SELECT * FROM {database}.{table};"
            cursor.execute(query)

            current_sheet = 1
            current_row_count = 0
            output_directory = "download"
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []

                for rows in generate_rows(cursor, max_rows_per_sheet):
                    file_path = os.path.join(output_directory, f"{output_file}_sheet{current_sheet}.csv")
                    future = executor.submit(export_mysql_data_chunk, host, user, password, database, table, header_info, rows, file_path)
                    futures.append(future)

                    current_row_count += len(rows)
                    total_rows_exported += len(rows)

                    if current_row_count >= max_rows_per_sheet:
                        current_sheet += 1
                        current_row_count = 0
                    print(f"{output_file}_sheet{current_sheet - 1}.csv�����ɹ�")

                # �ȴ������߳����
                concurrent.futures.wait(futures)

            print(f"�����ѳɹ������� {output_directory}.{output_file}")

    except Error as e:
        print(f"����: {e}")
        traceback.print_exc()

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("�����ѹر�")

    end_time = time.time()
    print(f'��������: {total_rows_exported} ������')
    print(f'����ʱ��: {end_time - start_time}��')

def generate_rows(cursor, max_rows_per_sheet):
    while True:
        rows = cursor.fetchmany(max_rows_per_sheet)
        if not rows:
            break
        yield rows

# ���ļ��ж�ȡ���ݿ���Ϣ
file_path = "database_info.txt"  # �滻������ļ�·��
db_info = read_mysql_info_from_file(file_path)

if __name__ == '__main__':
    if db_info:
        host, user, password, database, table, output_file = db_info
        max_rows_per_sheet = 1000000
        num_threads = 8 # �����߳���
        export_mysql_data_batch_threaded(host, user, password, database, table, output_file, max_rows_per_sheet, num_threads)
        time.sleep(10)
    else:
        print("��ȡ���ݿ���Ϣʧ�ܣ������ļ���")