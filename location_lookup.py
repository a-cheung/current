import time
from datetime import timedelta
import pandas as pd
import cx_Oracle

start = time.clock()

sheet = pd.read_excel("C:/Users/acheung/Desktop/norway_cluster2_clustering.xlsx", sheetname=0).replace('\'','%',regex=True)

zips = list(zip(sheet['ip'], sheet['city'], sheet['state'], sheet['country']))
results = []


connection = r"NGA/IPI_NGA@prodscan.prod.quova.com:1521/HM.quova"
conn = cx_Oracle.connect(connection)
c = conn.cursor()


for z in zips:
        output = c.execute(u"""
            SELECT a.city_id, a.city_name, b.state_name, c.country_name
            FROM new_central.city a
            JOIN new_central.state b
                ON a.state_id = b.state_id
            JOIN new_central.country c
                ON b.country_id = c.country_id
            WHERE a.city_name LIKE TRIM(LOWER('{city}')) AND b.state_name LIKE TRIM(LOWER('{state}')) AND c.country_name LIKE TRIM(LOWER('{country}'))""".format(city=z[1], state=z[2], country=z[3]))
        for o in output:
            results.append((z[0], o[0], z[1], z[2], z[3]))

print (results)





end = time.clock()
elapsed = str(timedelta(seconds=(end - start)))
print ("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))