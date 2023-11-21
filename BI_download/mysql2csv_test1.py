import mysql.connector
from mysql.connector import Error
import time
import os


def read_mysql_info_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf8') as file:
            lines = file.readlines()

            # ��ʼ���ֵ�
            variables = {}

            for line in lines:
                # ʹ�� split ����ȥ������������ȡֵ
                try:
                    var_name, var_value = map(str.strip, line.split('='))
                    variables[var_name] = var_value.strip('"')
                except Exception as e:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f"������ʱ����: {e}")

            return variables.get('host'), variables.get('user'), variables.get('password'), variables.get(
                'database'), variables.get('table'), variables.get('output_file')

    except Exception as e:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f"��ȡ�ļ�����: {e}")
        return None


def get_query_header_info(cursor):
    return [column[0] for column in cursor.description]


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
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "�ɹ����ӵ����ݿ�")

            cursor = connection.cursor()

            # # ��ȡ��ͷ��Ϣ
            # cursor.execute(f"DESCRIBE {table}")
            # header_info = [column[0] for column in cursor.fetchall()]

            table_name = input("�Ƿ��Զ��嵼�������ݣ���/�񣩣�")
            if table_name == '��':
                print("������SQL��䣬��';'����������������ɺ����λس���")
                sql_lines = []
                while True:
                    line = input()
                    if line.strip() == '':
                        break
                    sql_lines.append(line)

                sql = ' '.join(sql_lines)
                print(sql)
                cursor.execute(sql)
            elif table_name == '��':
                query = f"SELECT * FROM {database}.{table};"
                cursor.execute(query)
            else:
                print('�������󣡣�')
                pass


            # ��ȡ��ѯ����ı�ͷ��Ϣ
            header_info = get_query_header_info(cursor)

            # ʹ�����������л�ȡ����
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
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f'д��{file_path}')

            with open(file_path, 'w', encoding='utf-8') as file:
                # д�����
                file.write(','.join(header_info) + '\n')

                for rows in generate_rows():
                    for row in rows:
                        file.write(','.join(map(str, row)) + '\n')
                        current_row_count += 1
                        total_rows_exported += 1
                        # ÿ�δﵽ�������ʱ���л����µ� sheet
                        if current_row_count >= max_rows_per_sheet:
                            file.close()
                            current_sheet += 1

                            current_row_count = 0
                            file_path = os.path.join(output_directory, f"{output_file}_sheet{current_sheet}.csv")
                            file = open(file_path, 'w', encoding='utf-8')
                            # д����⵽�µ� sheet
                            file.write(','.join(header_info) + '\n')

                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                          f"{output_file}_sheet{current_sheet - 1}.csv�����ɹ�")
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f'�л�sheet{current_sheet}')
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  f"{output_file}_sheet{current_sheet}.csv�����ɹ�")
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  f'�����ѳɹ������� {output_directory}.{output_file}����')

    except Error as e:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f"����: {e}")


    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("�����ѹر�")

    end_time = time.time()

    print(f'��������: {total_rows_exported} ������')
    print(f'����ʱ��: {end_time - start_time}��')


# ���ļ��ж�ȡ���ݿ���Ϣ
file_path = "database_info.txt"  # �滻������ļ�·��
db_info = read_mysql_info_from_file(file_path)

if __name__ == '__main__':

    if db_info:
        # ������ݿ���Ϣ
        host, user, password, database, table, output_file = db_info

        # ����ÿ�� sheet ���������
        max_rows_per_sheet = 1000000

        print("*ע��* ÿһ��CSV�ļ�����100������")

        # ���õ�������
        export_mysql_data_batch(host, user, password, database, table, output_file, max_rows_per_sheet)
        time.sleep(10)
    else:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "��ȡ���ݿ���Ϣʧ�ܣ������ļ���")
