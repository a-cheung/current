import time
from datetime import timedelta
import pandas as pd
import re
import cx_Oracle

start = time.clock()

connection = r"NGA/IPI_NGA@prodscan.prod.quova.com:1521/HM.quova"
conn = cx_Oracle.connect(connection)
c = conn.cursor()

def main():
    start = time.clock()

    df = pd.read_excel('C:/Users/acheung/Documents/ATS/AIT20/InVentiv IP Target List for Digital Q2 2018.xlsx', sheetname=1).apply(lambda x: x.astype(str).str.lower())

    org_search(df)

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
    symbols = '/#$'
    for i, row in df.iterrows():
        o = '%'+row['org_name'].replace(' ','%')+'%'
        for s in symbols:
            o = o.replace(s,'%')
        org_searchterms.append((row['id'],o))
    print (org_searchterms)

# def search_terms(df):
#     for _, r in df.iterrows():



# main()





