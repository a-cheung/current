import time
from datetime import timedelta
import pandas as pd
import pandas.io.formats.excel
import re
import cx_Oracle

start = time.clock()

connection = r"NGA/IPI_NGA@prodscan.prod.quova.com:1521/HM.quova"
conn = cx_Oracle.connect(connection)
c = conn.cursor()

def main():
    start = time.clock()

    df = pd.read_excel('C:/Users/acheung/Documents/ATS/AIT20/InVentiv IP Target List for Digital Q2 2018.xlsx', sheetname=1).apply(lambda x: x.astype(str).str.lower())


    pd.io.formats.excel.header_style = None
    writer = pd.ExcelWriter("C:/Users/acheung/Documents/ATS/AIT20/search_results.xlsx")
    org_search(df).to_excel(writer, 'org_search', index=False)
    addr_search(df).to_excel(writer, 'addr_search', index=False)
    writer.save()


    end = time.clock()
    elapsed = str(timedelta(seconds=(end - start)))
    print("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))


def clean():
    for _, r in df.iterrows():
        nospace = str(r[0]).replace(' ','%')
        str_term = re.search('[0-9]*(.+)',nospace).group(1)
        if '#' in str_term:
            str_term = re.search('(.+)#.*',str_term).group(1)
        if 'apt' in str_term:
            str_term = re.search('(.+)apt.*', str_term).group(1)
        if 'unit' in str_term:
            str_term = re.search('(.+)unit.*', str_term).group(1)
        if 'lot' in str_term:
            str_term = re.search('(.+)lot.*', str_term).group(1)
        if 'ste' in str_term:

            str_term = re.search('(.+)ste.*', str_term).group(1)
        search_term = '%'+str_term+'%'+r['city']+'%'+r['state']+'%'+r['zip']+'%'
        print(search_term)

    output = c.execute(u"""
        SELECT octs(start_ip), octs(end_ip), organization_name, address
        FROM new_central.reginetnum
        WHERE address LIKE '{addr}'""".format(addr=search_term))

    export.append((search_term,r[0],r[1],r[2],r[3],output.fetchall()))


def org_search(df):
    org_searchterms = pd.DataFrame(columns=['id','searchterm'])
    org_results = pd.DataFrame(columns=['id','searchterm','start_ip','end_ip','org_id','org_name','city','state','country','addr'])
    symbols = "-'/#$"

    for i, row in df.iterrows():
        o = '%'+row['org_name'].replace(' ','%')+'%'
        for s in symbols:
            o = o.replace(s,'%')
        org_searchterms.loc[len(org_searchterms)]=(row['id'],o)

    org_searchterms = org_searchterms.drop_duplicates(subset='searchterm')

    for j, r in org_searchterms.iterrows():
        output = c.execute(u"""
            SELECT octs(a.start_ip), octs(a.end_ip), a.organization_id, o.organization_name, x.city_name, y.state_name, z.country_name
            FROM release_staging.assignment a
            JOIN new_central.organization o
                ON a.organization_id = o.organization_id
            JOIN new_central.city x
                ON a.city_id = x.city_id
            JOIN new_central.state y
                ON a.state_id = y.state_id
            JOIN new_central.country z
                ON a.country_id = z.country_id
            WHERE o.organization_name LIKE '{search}'""".format(search=r['searchterm']))
        for o in output.fetchall():
            org_results.loc[len(org_results)]=(r['id'],r['searchterm'],o[0],o[1],o[2],o[3],o[4],o[5],o[6],'')


        output = c.execute(u"""
            SELECT octs(start_ip), octs(end_ip), organization_id, organization_name, address
            FROM new_central.reginetnum
            WHERE organization_name LIKE '{search}'""".format(search=r['searchterm']))
        for p in output.fetchall():
            org_results.loc[len(org_results)]=(r['id'],r['searchterm'],p[0],p[1],p[2],p[3],'','','',p[4])

    print("org search done")
    return org_results


def addr_search(df):
    addr_searchterms = pd.DataFrame(columns=['id','searchterm'])
    addr_results = pd.DataFrame(columns=['id', 'searchterm', 'start_ip', 'end_ip', 'org_id', 'org_name','address'])
    symbols = "-'/#$"

    for i, row in df.iterrows():
        addr = row['address'].replace(' ', '%')
        for s in symbols:
            addr = addr.replace(s, '%')

        if row['city'] != 'nan':
            city_search = '%'+addr+'%'+row['city'].replace(' ', '%')+'%'+row['country']
            addr_searchterms.loc[len(addr_searchterms)]=(row['id'],city_search)
        if row['state'] != 'nan':
            state_search = '%'+addr+'%'+'state: '+row['state'].replace(' ', '%')+'%'+row['country']
            addr_searchterms.loc[len(addr_searchterms)] = (row['id'],state_search)
        if row['zip'] != 'nan':
            zip_search = '%'+addr+'%'+row['zip'].replace(' ', '%')+'%'+row['country']
            addr_searchterms.loc[len(addr_searchterms)] = (row['id'],zip_search)

    addr_searchterms = addr_searchterms.drop_duplicates(subset='searchterm')

    for j, r in addr_searchterms.iterrows():
        output = c.execute(u"""
                    SELECT octs(start_ip), octs(end_ip), organization_id, organization_name, address
                    FROM new_central.reginetnum
                    WHERE address LIKE '{search}'""".format(search=r['searchterm']))
        for o in output.fetchall():
            addr_results.loc[len(addr_results)] = (r['id'], r['searchterm'], o[0], o[1], o[2], o[3],o[4])

    print("addr search done")
    return addr_results


main()


import os



