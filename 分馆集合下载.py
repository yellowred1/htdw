import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
import plotly.graph_objects as go
import time
import datetime
from config import conn
from store_name import get_store_name

qibao = 29
qibao_name = get_store_name(qibao)
conn = conn()
cursor = conn.cursor()
sql = f"""
select a.*, b.performance, b.income
from (select distinct operating_income
                    , substring(cal_date, 1, 7) as cal_date
                    , people_cost
                    , site_cost
                    , other_cost
                    , headquarter_cost
                    , operating_people
                    , sales_people
                    , average_performance
                    , admission_student
      from business_analysis.ba_average_profits
      where store_id = {qibao}
      group by substring(cal_date, 1, 7)) a
         left join (select zr_month  as cal_date,
                           ifnull(sum(case when operation_type <> -1 and staff_id >= 0 then bcs_score else 0 end), 0) -
                           ifnull(sum(case when operation_type = -1 and staff_id >= -1 then bcs_score else 0 end),
                                  0) as performance,
                           ifnull(sum(case when operation_type <> -1 and staff_id >= 0 then bcs_score else 0 end),
                                  0) as income
                    from dw_bi.rp_dw_contract_sales
                    where 1 = 1
                      and store_id = {qibao}
                      AND staff_id in (0, -1)
                      and tenant_id = 1
                    GROUP by zr_month) b
                   on a.cal_date = b.cal_date;
"""
data = pd.read_sql(sql, conn)
conn.close()
cursor.close()
data['cal_date'] = range(1, len(data) + 1)

# ----------------------------------------------------------------------------------------------------
df = pd.DataFrame(data)
# 多元线性回归分析
X = df[
    ['cal_date', 'admission_student', 'people_cost', 'site_cost', 'other_cost', 'headquarter_cost', 'operating_people',
     'sales_people', 'average_performance']]
y = df['performance']

X = sm.add_constant(X)
model = sm.OLS(y, X).fit()  # todo mode
# ----------------------------------------------------------------------------------------------------
# 选择自变量和因变量
X = data[['cal_date', 'operating_people', 'sales_people']]  # 开馆时间和销售人数作为自变量
y = data['admission_student']  # 招生学员作为因变量

# 添加截距项
X = sm.add_constant(X)

# 拟合多元线性回归模型
model2 = sm.OLS(y, X).fit()  # todo model2 招生学员与运营收入关系

# ----------------------------------------------------------------------------------------------------
# 添加回归线
X = data['admission_student']
y = data['average_performance']
# 显示模型数据
model3 = sm.OLS(y, sm.add_constant(X)).fit()  # todo model3 人均利润与招生人数关系
# ----------------------------------------------------------------------------------------------------
# 选择自变量和因变量
X = data[['cal_date', 'operating_people', 'sales_people']]
y = data['people_cost']
X = sm.add_constant(X)
# 拟合多元线性回归模型
model10 = sm.OLS(y, X).fit()  # todo model10 开馆时间和运营人数和销售人数作为自变量


for q in range(1,66):
    other_store = q
    # print(other_store)
    file_name = get_store_name(other_store)
    from config import conn

    conn = conn()
    cursor = conn.cursor()

    sql_query = f"""
    select a.*, b.performance, b.income
    from (select distinct operating_income
                        , substring(cal_date, 1, 7) as cal_date
                        , people_cost
                        , site_cost
                        , other_cost
                        , headquarter_cost
                        , operating_people
                        , sales_people
                        , average_performance
                        , admission_student
          from business_analysis.ba_average_profits
          where store_id = {other_store}
          group by substring(cal_date, 1, 7)) a
             left join (select zr_month  as cal_date,
                               ifnull(sum(case when operation_type <> -1 and staff_id >= 0 then bcs_score else 0 end), 0) -
                               ifnull(sum(case when operation_type = -1 and staff_id >= -1 then bcs_score else 0 end),
                                      0) as performance,
                               ifnull(sum(case when operation_type <> -1 and staff_id >= 0 then bcs_score else 0 end),
                                      0) as income
                        from dw_bi.rp_dw_contract_sales
                        where 1 = 1
                          and store_id = {other_store}
                          AND staff_id in (0, -1)
                          and tenant_id = 1
                        GROUP by zr_month) b
                       on a.cal_date = b.cal_date
    """
    new_data = pd.read_sql_query(sql_query, conn)
    new_data['cal_date'] = range(1, len(new_data) + 1)
    conn.close()
    # print(new_data)
    # print(len(new_data))
    if len(new_data)<1:
        pass
    else:
        def predicted_income1(cal_date, admission_student, people_cost, site_cost, other_cost, headquarter_cost,
                              operating_people, sales_people, average_performance):
            return (
                    model.params[0] +
                    model.params[1] * cal_date +
                    model.params[2] * admission_student +
                    model.params[3] * people_cost +
                    model.params[4] * site_cost +
                    model.params[5] * other_cost +
                    model.params[6] * headquarter_cost +
                    model.params[7] * operating_people +
                    model.params[8] * sales_people +
                    model.params[9] * average_performance
            )

        # 预测运营收入
        predicted_incomes_new = [
            predicted_income1(
                row['cal_date'],
                row['admission_student'],
                row['people_cost'],
                row['site_cost'],
                row['other_cost'],
                row['headquarter_cost'],
                row['operating_people'],
                row['sales_people'],
                row['average_performance']
            )
            for index, row in new_data.iterrows()
        ]

        # 获取当前数据表的最大cal_date值
        max_cal_date = new_data['cal_date'].max()

        # 计算需要添加的行数
        ds = 2
        num_rows_to_add = max_cal_date + ds

        # 计算需要填充的平均值（接下来的12个月的平均值）
        new_rows = []
        for i in range(max_cal_date + 1, max_cal_date + ds + 1):
            last_12_months_data = new_data.tail(12)
            last_1_months_data = new_data.tail(12)
            avg_people_cost = last_12_months_data['people_cost'].mean()
            avg_site_cost = last_12_months_data['site_cost'].mean()
            avg_other_cost = last_12_months_data['other_cost'].mean()
            avg_headquarter_cost = last_12_months_data['headquarter_cost'].mean()
            avg_operating_people = last_12_months_data['operating_people'].mean()
            avg_sales_people = last_12_months_data['sales_people'].mean()
            avg_average_performance = last_12_months_data['average_performance'].mean()
            admission_student = last_1_months_data['admission_student'].mean()

            new_row = {
                'cal_date': i,
                'people_cost': model10.predict([1, i, avg_operating_people, avg_sales_people])[0],
                # 'people_cost': avg_people_cost,
                'site_cost': avg_site_cost,
                'other_cost': avg_other_cost,
                'headquarter_cost': avg_headquarter_cost,
                'operating_people': avg_operating_people,
                'sales_people': avg_sales_people,
                'average_performance': model3.predict([1, admission_student])[0],  # todo 切换模型
                # 'average_performance': 40494.935,
                'admission_student': model2.predict([1, i, avg_operating_people, avg_sales_people])[0],
                # 'admission_student': admission_student,
            }
            new_rows.append(new_row)
            # new_data = pd.concat([new_data, pd.DataFrame(new_rows)], ignore_index=True)

            # 只保留最新的行，去掉之前添加的行
            new_data = pd.concat([new_data.head(max_cal_date), new_data.tail(0), pd.DataFrame(new_rows)], ignore_index=True)
            ds += 1

        # 打印处理后的数据，仅包含最新的行
        # new_data.head(100)

        # 预测高新的运营收入
        new_data['predicted_income'] = new_data.apply(
            lambda row: predicted_income1(
                row['cal_date'],
                row['admission_student'],
                row['people_cost'],
                row['site_cost'],
                row['other_cost'],
                row['headquarter_cost'],
                row['operating_people'],
                row['sales_people'],
                row['average_performance']
            ),
            axis=1
        )
        new_data['store'] = f'{q}'


        from config import conn
        conn = conn()
        cursor = conn.cursor()
        sql_query = f"""select distinct operating_income
                    , substring(cal_date, 1, 7) as cal_date
                    , substring(cal_date, 1, 7) as cal_date_month
      from business_analysis.ba_average_profits
      where store_id = {other_store}
      group by substring(cal_date, 1, 7)
           """
        oder_data = pd.read_sql_query(sql_query, conn)
        oder_data['cal_date'] = range(1, len(oder_data) + 1)
        conn.close()
        data_new = new_data.set_index('cal_date').join(oder_data.set_index('cal_date'),on='cal_date',rsuffix ='1')


        data_new.iloc[-4:].to_csv('download\\分馆65-1.csv', mode='a', header=True)
        print(f'插入成功分馆{q}')