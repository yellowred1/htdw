import mysql.connector
from mysql.connector import Error
import time
import os
import traceback

def read_mysql_info_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf8') as file:
            lines = file.readlines()
            variables = {}

            for line in lines:
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

def export_mysql_data_batch(host, user, password, database, table, output_file, max_rows_per_sheet=1000000):
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

            def generate_rows():
                while True:
                    rows = cursor.fetchmany(max_rows_per_sheet)
                    if not rows:
                        break
                    yield rows

            current_sheet = 1
            current_row_count = 0
            output_directory = "download"
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)

            file_path = os.path.join(output_directory, f"{output_file}_sheet{current_sheet}.csv")

            with open(file_path, 'w', encoding='utf8') as file:
                file.write(','.join(header_info) + '\n')

                for rows in generate_rows():
                    for row in rows:
                        file.write(','.join(map(str, row)) + '\n')
                        current_row_count += 1
                        total_rows_exported += 1
                        if current_row_count >= max_rows_per_sheet:
                            current_sheet += 1
                            current_row_count = 0
                            file_path = os.path.join(output_directory, f"{output_file}_sheet{current_sheet}.csv")
                            with open(file_path, 'w', encoding='utf8') as new_file:
                                new_file.write(','.join(header_info) + '\n')

                print(f"{output_file}_sheet{current_sheet}.csv�����ɹ�")

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

if __name__ == '__main__':
    file_path = "database_info.txt"  # �滻������ļ�·��
    db_info = read_mysql_info_from_file(file_path)

    if db_info:
        host, user, password, database, table, output_file = db_info
        max_rows_per_sheet = 1000000
        export_mysql_data_batch(host, user, password, database, table, output_file, max_rows_per_sheet)
    else:
        print("��ȡ���ݿ���Ϣʧ�ܣ������ļ���")
