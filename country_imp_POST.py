import time
from datetime import timedelta
import pandas as pd
import cx_Oracle

start = time.clock()


file_name = "C:/Users/acheung/Desktop/finland_concat_PRE_clustering.csv"
df = pd.read_csv(file_name)
print(df.columns)

connection = r"NGA/IPI_NGA@prodscan.prod.quova.com:1521/HM.quova"
conn = cx_Oracle.connect(connection)
c = conn.cursor()


db_out = []
location = []


for ip in df['ip']:
    ip_oct = ip
    parts = ip_oct.split('.')
    ip_int = (int(parts[0]) << 24) + (int(parts[1]) << 16) + \
             (int(parts[2]) << 8) + int(parts[3])
    print(ip_int)

    output_db = c.execute(u"""
        SELECT a.cidr, y.city_id, y.city_name, s.state_name, c.country_name, a.asn_id, n.asname, o.organization_name, a.colo_status_id, n.connectiontype, a.connectiontype_id, r.ip_routingtype, a.ip_routingtype_id
        FROM release_staging.assignment a
        LEFT OUTER JOIN new_central.asnumber n
            ON a.asn_id = n.asn
        LEFT OUTER JOIN new_central.organization o
            ON a.organization_id = o.organization_id
        LEFT OUTER JOIN new_central.country c
            ON a.country_id = c.country_id
        LEFT OUTER JOIN new_central.state s
            ON a.state_id = s.state_id
        LEFT OUTER JOIN new_central.city y
            ON a.city_id = y.city_id
        LEFT OUTER JOIN new_central.connectiontype n
            ON a.connectiontype_id = n.connectiontype_id
        LEFT OUTER JOIN new_central.ip_routingtype r
            ON a.ip_routingtype_id = r.ip_routingtype_id
        WHERE start_ip = {start_int}""".format(start_int=ip_int))
    for odb in output_db:
        db_out.append((ip_oct,ip_int,odb[0],odb[1],odb[2],odb[3],odb[4],odb[5],odb[6],odb[7],odb[8],odb[9],odb[10],odb[11],odb[12]))


for index, row in df.iterrows():
    xx = []
    if str(row['clusterCity']) != 'nan':
        loc = str(row['clusterCity']).replace('\'', '%').split(',')
        output_loc = c.execute(u"""
               SELECT a.city_id, a.city_name, b.state_name, c.country_name
               FROM new_central.city a
               JOIN new_central.state b
                   ON a.state_id = b.state_id
               JOIN new_central.country c
                   ON b.country_id = c.country_id
               WHERE a.city_name LIKE TRIM(LOWER('{city}')) AND b.state_name LIKE TRIM(LOWER('{state}')) AND c.country_name LIKE TRIM(LOWER('{country}'))""".format(
            city=loc[0], state=loc[1], country=loc[2]))
        for ol in output_loc:
            xx.append(ol)
        if len(xx) > 0:
            for x in xx:
                location.append((row['ip'],x[0],x[1],x[2],x[3]))
        else:
            location.append((row['ip'],'N/A',loc[0],loc[1],loc[2]))
    else:
        location.append((row['ip'],'N/A','N/A','N/A','N/A'))


db_out_df = pd.DataFrame(db_out, columns=['ip_oct','ip_int','cidr','gds_city_id','gds_city','gds_state','gds_country','asn_id','asname','organization','colo_status','connectiontype','ct_id','routingtype','rt_id'])
location_df = pd.DataFrame(location, columns=['ip_oct','cluster_city_id','cluster_city','cluster_state','cluster_country'])
merge_df = pd.merge(db_out_df, location_df, on=['ip_oct'])
export_df = merge_df[['ip_oct','ip_int','cidr','cluster_city_id','cluster_city','cluster_state','cluster_country','gds_city_id','gds_city','gds_state','gds_country','asn_id','asname','organization','colo_status','connectiontype','ct_id','routingtype','rt_id']]
export_df.to_csv("{file}POST.csv".format(file=file_name[:-18]))


end = time.clock()
elapsed = str(timedelta(seconds=(end - start)))
print ("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))