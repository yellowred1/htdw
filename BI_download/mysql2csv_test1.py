import mysql.connector
from mysql.connector import Error
import time
import os


def read_mysql_info_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf8') as file:
            lines = file.readlines()

            # 初始化字典
            variables = {}

            for line in lines:
                # 使用 split 函数去掉变量名并获取值
                try:
                    var_name, var_value = map(str.strip, line.split('='))
                    variables[var_name] = var_value.strip('"')
                except Exception as e:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f"处理行时出错: {e}")

            return variables.get('host'), variables.get('user'), variables.get('password'), variables.get(
                'database'), variables.get('table'), variables.get('output_file')

    except Exception as e:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f"读取文件错误: {e}")
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
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "成功连接到数据库")

            cursor = connection.cursor()

            # # 获取表头信息
            # cursor.execute(f"DESCRIBE {table}")
            # header_info = [column[0] for column in cursor.fetchall()]

            table_name = input("是否自定义导出表数据（是/否）：")
            if table_name == '是':
                print("请输入SQL语句，以';'结束。多行输入完成后按两次回车：")
                sql_lines = []
                while True:
                    line = input()
                    if line.strip() == '':
                        break
                    sql_lines.append(line)

                sql = ' '.join(sql_lines)
                print(sql)
                cursor.execute(sql)
            elif table_name == '否':
                query = f"SELECT * FROM {database}.{table};"
                cursor.execute(query)
            else:
                print('输入有误！！')
                pass


            # 获取查询结果的表头信息
            header_info = get_query_header_info(cursor)

            # 使用生成器逐行获取数据
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
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f'写入{file_path}')

            with open(file_path, 'w', encoding='utf-8') as file:
                # 写入标题
                file.write(','.join(header_info) + '\n')

                for rows in generate_rows():
                    for row in rows:
                        file.write(','.join(map(str, row)) + '\n')
                        current_row_count += 1
                        total_rows_exported += 1
                        # 每次达到最大行数时，切换到新的 sheet
                        if current_row_count >= max_rows_per_sheet:
                            file.close()
                            current_sheet += 1

                            current_row_count = 0
                            file_path = os.path.join(output_directory, f"{output_file}_sheet{current_sheet}.csv")
                            file = open(file_path, 'w', encoding='utf-8')
                            # 写入标题到新的 sheet
                            file.write(','.join(header_info) + '\n')

                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                          f"{output_file}_sheet{current_sheet - 1}.csv导出成功")
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f'切换sheet{current_sheet}')
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  f"{output_file}_sheet{current_sheet}.csv导出成功")
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  f'数据已成功导出到 {output_directory}.{output_file}集合')

    except Error as e:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), f"错误: {e}")


    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("连接已关闭")

    end_time = time.time()

    print(f'导出总数: {total_rows_exported} 条数据')
    print(f'导出时间: {end_time - start_time}秒')


# 从文件中读取数据库信息
file_path = "database_info.txt"  # 替换成你的文件路径
db_info = read_mysql_info_from_file(file_path)

if __name__ == '__main__':

    if db_info:
        # 解包数据库信息
        host, user, password, database, table, output_file = db_info

        # 设置每个 sheet 的最大行数
        max_rows_per_sheet = 1000000

        print("*注意* 每一个CSV文件保存100万条数")

        # 调用导出函数
        export_mysql_data_batch(host, user, password, database, table, output_file, max_rows_per_sheet)
        time.sleep(10)
    else:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "读取数据库信息失败，请检查文件。")
