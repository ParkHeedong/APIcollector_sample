import pandas


date_index = pandas.date_range(start='20210601', end = '20210615')
date_list = date_index.strftime("%Y%m%d").tolist()
print(date_list)