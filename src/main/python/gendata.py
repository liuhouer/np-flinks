import random

# 机构等级
hos_levels = ['一级：1', '二级：2', '三级：3']

# 收入类别
income_categories = ['药品收入：yp', '耗材收入：hc', '检查检验：jcjy', '化验：hy', '医疗服务：yl', '其他：qt']

# 机构编码和名称
hos_codes = ['hos001', 'hos002', 'hos003', 'hos004']
hos_names = ['医院A', '医院B', '医院C', '医院D']

# 生成insert语句
for i in range(36):
    year_mon = f'2019{i+1:02d}'
    for j in range(100):
        hos_level = random.choice(hos_levels)
        hos_code = random.choice(hos_codes)
        hos_name = random.choice(hos_names)
        income_category = random.choice(income_categories)
        income_amount = round(random.uniform(1000, 10000), 2)
        sql = f"INSERT INTO med_ins_income (year, year_mon, hos_level, hos_code, hos_name, income_category, income_amount) VALUES ('2019', '{year_mon}', '{hos_level}', '{hos_code}', '{hos_name}', '{income_category}', {income_amount});"
        print(sql)