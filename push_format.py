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

    file_name = "C:/Users/acheung/Desktop/duplicates_050118.xlsx"
    # df = pd.read_csv(file_name)
    df = pd.read_excel(file_name,sheetname='review')

    ###Sony
    # file_name = "C:/Users/acheung/Documents/sony/sony_04272018_work_clustering.csv"
    # df = pd.read_csv(file_name)
    # filter = (df['decision'] == 'customer') | (df['decision'] == 'unchanged')
    # df = df[filter]

    ###MLB
    # file_name = "C:/Users/acheung/Documents/mlb/mlb_work_043018_clustering.csv"
    # df = pd.read_csv(file_name)
    # filter = ((df['decision'] == 'customer') & (df['topClusterScore'] > 1)) | (df['decision'] == 'unchanged')
    # df = df[filter]


    print("file length: {0}".format(len(df)))

    emp_id = "539"
    lookup_type = "idexists"
    ip_type = "start"

    variables(lookup_type,ip_type)
    db = database(df,ip_type)
    loc = loc_lookup(lookup_type,df)

    export(db,loc,emp_id,file_name)


    end = time.clock()
    elapsed = str(timedelta(seconds=(end - start)))
    print("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))

def variables(lookup_type,ip_type):
    if lookup_type not in ('CITY','COUNTRY','clustercity','idexists'):
        print("Invalid lookup_type, use: 'CITY', 'COUNTRY', 'clustercity' or 'idexists'")
        quit()

    if ip_type not in ('start','exact'):
        print("Invalid ip_type, use: 'start' or 'exact'")
        quit()

    print("variables valid")


def database(df,ip_type):
    cidr = pd.DataFrame(columns=['ip_oct','ip_int','octs1','octs2','octs3','octs4','v_cidr'])
    if ip_type == 'exact':
        cidr = pd.DataFrame(columns=['ip_oct','ip_int','start_oct','start_int','octs1','octs2','octs3','octs4','v_cidr'])
    ct_rt = pd.DataFrame(columns=['ip_oct','propvalue1','propvalue2','asn_id','asname','org'])

    for ip_oct in df['ip']:
        parts = ip_oct.split('.')
        ip_int = (int(parts[0]) << 24) + (int(parts[1]) << 16) + \
                 (int(parts[2]) << 8) + int(parts[3])

        if ip_type == 'start':
            cidr_db = c.execute(u"""
                SELECT a.cidr
                FROM new_central.assignment a
                WHERE start_ip = {start_int}""".format(start_int=ip_int))
            for cdb in cidr_db:
                cidr.loc[len(cidr)] = [ip_oct, ip_int, parts[0], parts[1], parts[2], parts[3], cdb[0]]
            start_int = ip_int


        elif ip_type == 'exact':
            cidr_db = c.execute(u"""
                SELECT octs(start_ip), start_ip, cidr
                FROM new_central.assignment
                WHERE classc_id = classc_id({ip_int}) AND start_ip <= {ip_int} AND end_ip >= {ip_int}""".format(ip_int=ip_int))
            for cdb in cidr_db:
                parts = cdb[0].split('.')
                cidr.loc[len(cidr)] = [ip_oct, ip_int, cdb[0], cdb[1], parts[0], parts[1], parts[2], parts[3], cdb[2]]
            start_int = cidr.iloc[len(cidr)-1]['start_int']

        rsa_db = c.execute(u"""
                        SELECT a.connectiontype_id, a.ip_routingtype_id, a.asn_id, n.asname, o.organization_name
                        FROM release_staging.assignment a
                        LEFT OUTER JOIN new_central.asnumber n
                            ON a.asn_id = n.asn
                        LEFT OUTER JOIN new_central.organization o
                            ON a.organization_id = o.organization_id
                        WHERE start_ip = {start_int}""".format(start_int=start_int))
        for rdb in rsa_db:
            ct_rt.loc[len(ct_rt)] = [ip_oct, rdb[0], rdb[1], rdb[2], rdb[3], rdb[4]]

    print("database lookup done")
    database = pd.merge(cidr,ct_rt,on=['ip_oct'])
    return database


def loc_lookup(lookup_type,df):
    location = pd.DataFrame(columns=['ip_oct','source','id'])

    if lookup_type == 'CITY':
        for index, row in df.iterrows():
            loc_db = c.execute(u"""
                SELECT a.city_id, a.city_name, b.state_name, c.country_name
                FROM new_central.city a
                JOIN new_central.state b
                    ON a.state_id = b.state_id
                JOIN new_central.country c
                    ON b.country_id = c.country_id
                WHERE a.city_name LIKE TRIM(LOWER('{city}')) AND b.state_name LIKE TRIM(LOWER('{state}')) AND c.country_name LIKE TRIM(LOWER('{country}'))""".format(city=row['city'], state=row['state'], country=row['country']))
            for ldb in loc_db:
                location.loc[len(location)] = [row['ip'],lookup_type,ldb[0]]

    elif lookup_type == 'COUNTRY':
        for index, row in df.iterrows():
            loc_db = c.execute(u"""
                SELECT country_id, country_name
                FROM new_central.country
                WHERE country_name LIKE TRIM(LOWER('{country}'))""".format(country=row['country']))
            for ldb in loc_db:
                location.loc[len(location)] = [row['ip'],lookup_type,ldb[0]]

    elif lookup_type == 'clustercity':
        lookup_type = 'CITY'
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
                location.loc[len(location)] = [row['ip'],lookup_type,ldb[0]]

    elif lookup_type == 'idexists':
        location = df[['ip','source','location_id']].rename(columns={'ip':'ip_oct','location_id':'id'})

    print("location lookup done")
    return location


def export(database,location,emp_id,file_name):
    merge = pd.merge(database, location, on=['ip_oct'])

    if 'start_oct' in merge:
        merge = merge[['octs1','octs2','octs3','octs4','v_cidr','propvalue1','propvalue2','source','id','start_oct','start_int','ip_oct','ip_int','asn_id','asname','org']]
        duplicate = merge.duplicated(subset=['start_oct']) == True
        print("\nduplicates removed:\n",merge[duplicate]['start_oct'],"\n")
        merge = merge.drop_duplicates(subset=['start_oct'])
    else:
        merge = merge[['octs1','octs2','octs3','octs4','v_cidr','propvalue1','propvalue2','source','id','ip_oct','ip_int','asn_id','asname','org']]
        duplicate = merge.duplicated(subset=['ip_oct']) == True
        print("\nduplicates removed:\n",merge[duplicate]['ip_oct'],"\n")
        merge = merge.drop_duplicates(subset=['ip_oct'])

    print("final file length: {0}".format(len(merge)))
    merge.insert(loc=5, column='empid', value=emp_id)
    merge.insert(loc=6, column='propname1', value='CONNECTION')
    merge.insert(loc=8, column='propname2', value='IPROUTING')
    merge.insert(loc=10, column='update_cf_flag', value=0)
    merge.insert(loc=13, column='hm_algtype', value='HM_REGULAR')
    merge.insert(loc=14, column='iprouteid', value=merge['propvalue2'])
    merge.insert(loc=15, column='hm_cf_estid', value=5)
    merge.insert(loc=16, column='entry_date', value=(time.strftime("%m/%d/%Y")))

    #for MLB only
    # cf = pd.read_csv("C:/Users/acheung/Documents/mlb/mlb_work_043018.csv",usecols=['IP_ADDRESS','GPP_CITY_CF'])
    # merge = pd.merge(merge,cf,left_on='ip_oct',right_on='IP_ADDRESS',how='left').drop(['IP_ADDRESS'],axis=1)

    pd.io.formats.excel.header_style = None
    writer = pd.ExcelWriter("{file}_PUSH.xlsx".format(file=file_name[:-4]))
    merge.to_excel(writer, 'Sheet1', index=False)
    writer.save()


main()