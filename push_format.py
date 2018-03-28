import time
from datetime import timedelta
import pandas as pd
import pandas.io.formats.excel
import cx_Oracle


connection = r"NGA/IPI_NGA@prodscan.prod.quova.com:1521/HM.quova"
conn = cx_Oracle.connect(connection)
c = conn.cursor()


def main():
    start = time.clock()

    file_name = "C:/Users/acheung/Documents/sony/sony_03232018_work_clustering_customer.csv"
    df = pd.read_csv(file_name)
    emp_id = "539"
    lookup_type = "idexists"

    if lookup_type == 'CITY':
        location = city_lookup(df)
    elif lookup_type == 'COUNTRY':
        location = country_lookup(df)
    elif lookup_type == 'clustercity':
        lookup_type = 'CITY'
        location = clustercity_lookup(df)
    elif lookup_type == 'idexists':
        source = input("Input source type, 'CITY' or 'COUNTRY': ")
        if source == 'CITY':
            lookup_type = 'CITY'
        elif source == 'COUNTRY':
            lookup_type = 'COUNTRY'
        else:
            print("Invalid source type, input 'CITY' or 'COUNTRY'")
            quit()
        location = df[['ip','location_id']].rename(columns={'ip':'ip_oct'})
    else:
        print("Invalid lookup_type, use: 'CITY', 'COUNTRY', 'clustercity' or 'idexists'")
        quit()


    cidr = pd.DataFrame(columns=['ip_oct','octs1','octs2','octs3','octs4','cidr'])
    ct_rt = pd.DataFrame(columns=['ip_oct','connection','routing','asn_id','asname','org'])


    for ip_oct in df['ip']:
        parts = ip_oct.split('.')
        ip_int = (int(parts[0]) << 24) + (int(parts[1]) << 16) + \
                (int(parts[2]) << 8) + int(parts[3])

        cidr_db = c.execute(u"""
            SELECT a.cidr
            FROM new_central.assignment a
            WHERE start_ip = {start_int}""".format(start_int=ip_int))
        for cdb in cidr_db:
            cidr.loc[len(cidr)] = [ip_oct, parts[0], parts[1], parts[2],parts[3], cdb[0]]

        rsa_db = c.execute(u"""
            SELECT a.connectiontype_id, a.ip_routingtype_id, a.asn_id, n.asname, o.organization_name
            FROM release_staging.assignment a
            LEFT OUTER JOIN new_central.asnumber n
                ON a.asn_id = n.asn
            LEFT OUTER JOIN new_central.organization o
                ON a.organization_id = o.organization_id
            WHERE start_ip = {start_int}""".format(start_int=ip_int))
        for rdb in rsa_db:
            ct_rt.loc[len(ct_rt)]=[ip_oct,rdb[0],rdb[1],rdb[2],rdb[3],rdb[4]]


    merge = pd.merge(pd.merge(cidr, ct_rt, on=['ip_oct']), location, on=['ip_oct']).rename(columns={'cidr':'v_cidr','connection':'propvalue1','routing':'propvalue2'})
    merge.insert(loc=6, column='empid', value=emp_id)
    merge.insert(loc=7, column='propname1', value='CONNECTION')
    merge.insert(loc=9, column='propname2', value='IPROUTING')
    merge.insert(loc=11, column='update_cf_flag', value=0)
    merge.insert(loc=12, column='source', value=lookup_type)
    merge.insert(loc=13, column='id', value=merge['location_id'])
    merge.insert(loc=14, column='hm_algtype', value='HM_REGULAR')
    merge.insert(loc=15, column='iprouteid', value=merge['propvalue2'])
    merge.insert(loc=16, column='hm_cf_estid', value=5)
    merge.insert(loc=17, column='entry_date', value=(time.strftime("%m/%d/%Y")))
    merge.insert(loc=18, column='octs', value=merge['ip_oct'])
    del merge['location_id']
    del merge['ip_oct']

    pd.io.formats.excel.header_style = None
    writer = pd.ExcelWriter("{file}_PUSH.xlsx".format(file=file_name[:-4]))
    merge.to_excel(writer, 'Sheet1', index=False)
    writer.save()

    end = time.clock()
    elapsed = str(timedelta(seconds=(end - start)))
    print("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))


def clustercity_lookup(df):
    location = pd.DataFrame(columns=['ip_oct','location_id'])

    for index, row in df.iterrows():
        loc = str(row['clusterCity']).replace('\'', '%').split(',')
        loc_db = c.execute(u"""
            SELECT a.city_id, a.city_name, b.state_name, c.country_name
            FROM new_central.city a
            JOIN new_central.state b
                ON a.state_id = b.state_id
            JOIN new_central.country c
                ON b.country_id = c.country_id
            WHERE a.city_name LIKE TRIM(LOWER('{city}')) AND b.state_name LIKE TRIM(LOWER('{state}')) AND c.country_name LIKE TRIM(LOWER('{country}'))""".format(
            city=loc[0], state=loc[1], country=loc[2]))
        for ldb in loc_db:
            location.loc[len(location)]=[row['ip'],ldb[0]]

    return location


def city_lookup(df):
    location = pd.DataFrame(columns=['ip_oct','location_id'])

    for index, row in df.iterrows():
        loc_db = c.execute(u"""
            SELECT a.city_id, a.city_name, b.state_name, c.country_name
            FROM new_central.city a
            JOIN new_central.state b
                ON a.state_id = b.state_id
            JOIN new_central.country c
                ON b.country_id = c.country_id
            WHERE a.city_name LIKE TRIM(LOWER('{city}')) AND b.state_name LIKE TRIM(LOWER('{state}')) AND c.country_name LIKE TRIM(LOWER('{country}'))""".format(
            city=row['city'], state=row['state'], country=row['country']))
        for ldb in loc_db:
            location.loc[len(location)]=[row['ip'],ldb[0]]

    return location


def country_lookup(df):
    location = pd.DataFrame(columns=['ip_oct','location_id'])

    for index, row in df.iterrows():
        loc_db = c.execute(u"""
            SELECT country_id, country_name
            FROM new_central.country
            WHERE country_name LIKE TRIM(LOWER('{country}'))""".format(country=row['country']))
        for ldb in loc_db:
            location.loc[len(location)]=[row['ip'],ldb[0]]

    return location


main()
