import time
from datetime import timedelta
import pandas as pd
import cx_Oracle

start = time.clock()

# df = pd.read_excel("C:/Users/acheung/Desktop/norway_cluster2_clustering.xlsx", sheetname=0).replace('\'','%',regex=True)
df = pd.read_csv("C:/Users/acheung/Desktop/sony_0313_0320/oodma_tocluster_clustering.csv")
location = pd.DataFrame(columns=['ip_oct','city','state','country','dma'])

connection = r"NGA/IPI_NGA@prodscan.prod.quova.com:1521/HM.quova"
conn = cx_Oracle.connect(connection)
c = conn.cursor()

for index, row in df.iterrows():
    # loc_db = c.execute(u"""
    #     SELECT a.city_id, a.city_name, b.state_name, c.country_name
    #     FROM new_central.city a
    #     JOIN new_central.state b
    #         ON a.state_id = b.state_id
    #     JOIN new_central.country c
    #         ON b.country_id = c.country_id
    #     WHERE a.city_name LIKE TRIM(LOWER('{city}')) AND b.state_name LIKE TRIM(LOWER('{state}')) AND c.country_name LIKE TRIM(LOWER('{country}'))""".format(
    #     city=loc[0], state=loc[1], country=loc[2]))
    xx = []

    if str(row['clusterCity']) != 'nan':
        loc = str(row['clusterCity']).replace('\'', '%').split(',')
        loc_db = c.execute(u"""
            SELECT DISTINCT a.city_id, a.city_name, b.state_name, c.country_name, l.dma_id
            FROM new_central.city a
            JOIN new_central.state b
                ON a.state_id = b.state_id
            JOIN new_central.country c
                ON b.country_id = c.country_id
            LEFT OUTER JOIN new_central.location l
                ON a.city_id = l.city_id
            WHERE a.city_name LIKE TRIM(LOWER('{city}')) AND b.state_name LIKE TRIM(LOWER('{state}')) AND c.country_name LIKE TRIM(LOWER('{country}'))""".format(
            city=loc[0], state=loc[1], country=loc[2]))
        for ldb in loc_db:
            xx.append(ldb)
        if len(xx) > 0:
            for x in xx:
                location.loc[len(location)] = [row['ip'],x[1],x[2],x[3],x[4]]
                print(len(location))

location.to_csv("C:/Users/acheung/Desktop/sony_0313_0320/oodma_tocluster_location.csv",index=False)


end = time.clock()
elapsed = str(timedelta(seconds=(end - start)))
print ("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))