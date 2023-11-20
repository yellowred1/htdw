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

            # 初始化字典
            variables = {}

            for line in lines:
                # 使用 split 函数去掉变量名并获取值
                try:
                    var_name, var_value = map(str.strip, line.split('='))
                    variables[var_name] = var_value.strip('"')
                except Exception as e:
                    print(f"处理行时出错: {e}")

            return variables.get('host'), variables.get('user'), variables.get('password'), variables.get(
                'database'), variables.get('table'), variables.get('output_file')

    except Exception as e:
        print(f"读取文件错误: {e}")
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
            print("成功连接到数据库")

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
                    print(f"{output_file}_sheet{current_sheet - 1}.csv导出成功")

                # 等待所有线程完成
                concurrent.futures.wait(futures)

            print(f"数据已成功导出到 {output_directory}.{output_file}")

    except Error as e:
        print(f"错误: {e}")
        traceback.print_exc()

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("连接已关闭")

    end_time = time.time()
    print(f'导出总数: {total_rows_exported} 条数据')
    print(f'导出时间: {end_time - start_time}秒')

def generate_rows(cursor, max_rows_per_sheet):
    while True:
        rows = cursor.fetchmany(max_rows_per_sheet)
        if not rows:
            break
        yield rows

# 从文件中读取数据库信息
file_path = "database_info.txt"  # 替换成你的文件路径
db_info = read_mysql_info_from_file(file_path)

if __name__ == '__main__':
    if db_info:
        host, user, password, database, table, output_file = db_info
        max_rows_per_sheet = 1000000
        num_threads = 8 # 调整线程数
        export_mysql_data_batch_threaded(host, user, password, database, table, output_file, max_rows_per_sheet, num_threads)
        time.sleep(10)
    else:
        print("读取数据库信息失败，请检查文件。")