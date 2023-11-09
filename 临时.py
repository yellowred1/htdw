import time
now = time.localtime()
# print(now)
# time.struct_time(tm_year=2019, tm_mon=9, tm_mday=3, tm_hour=19, tm_min=20, tm_sec=34, tm_wday=1, tm_yday=246, tm_isdst=0)
# 转年月日 时分秒
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
	# '2019-09-03 15:48:21'


import datetime
date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
print(date)
# datetime.datetime(2019, 9, 3, 15, 48, 21, 41734)
# 转年月日 时分秒
# print(date.strftime("%Y-%m-%d %H:%M:%S"))
	# '2019-09-03 15:48:21'
