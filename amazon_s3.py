import time
from datetime import timedelta
import pandas as pd
import boto3
import botocore
import re
import io

start = time.clock()

# df = pd.read_excel("C:/Users/acheung/Desktop/sony_0313_0320/gpp_s3.xlsx",sheetname=1,dtype={'bblock':object})
# df = pd.read_csv("C:/Users/acheung/Documents/mlb/archive/mlb_work_043018.csv")
# print("file size: {}".format(len(df)))
ip = ['170.253.235.161',
'172.3.135.237',
'172.10.132.67',
'172.10.132.67',
'172.16.12.242',
'172.16.13.7',
'172.16.13.83',
'172.16.14.53']
export = pd.DataFrame(columns=['ip','start_oct','end_oct','country','state','city','zip'])
dne = []

# setup S3 Connection
s3 = boto3.client('s3')
bucket = "neustar-ipi"


for ip_oct in ip:#df['ip']:
    parts = ip_oct.split('.')
    ip_int = (int(parts[0]) << 24) + (int(parts[1]) << 16) + (int(parts[2]) << 8) + int(parts[3])

    bblock = re.match('^([0-9]+\.[0-9]+)',ip_oct).group(1)
    key = "prod/archive180/analysis/INDEXED/gpp/GPPPipeline_v888_20180502224305-03-2018-01-05-62/{index}.csv".format(index=bblock)

    try:
        s3.Bucket(bucket).download_file(key,"C:/Users/acheung/Desktop/sony_0313_0320/releases/gps_releases_other/{index}.csv".format(index=index))
        # obj = s3.get_object(Bucket=bucket, Key=key)
        #
        # columns = ['start_int','end_int','start_oct','end_oct','country','state','city','zip']
        # df = pd.read_csv(io.BytesIO(obj['Body'].read()),header=None,names=columns,usecols=[0,1,2,3,5,9,12,50])
        # exact = df[df['start_int']==ip_int]
        # between = df[(df['start_int']<=ip_int) & (df['end_int']>=ip_int)]
        #
        # if not exact.empty:
        #     export.loc[len(export)]=[ip_oct,exact.iloc[0]['start_oct'],exact.iloc[0]['end_oct'],exact.iloc[0]['country'],exact.iloc[0]['state'],exact.iloc[0]['city'],exact.iloc[0]['zip']]
        #
        # elif not between.empty:
        #     export.loc[len(export)] = [ip_oct,between.iloc[0]['start_oct'], between.iloc[0]['end_oct'],between.iloc[0]['country'], between.iloc[0]['state'], between.iloc[0]['city'],between.iloc[0]['zip']]
        #
        # else:
        #     print('IP: {} not found in file'.format(ip_oct))

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The file {index} does not exist.".format(index=index))
            dne.append(index)
        else:
            raise

    print(len(export))


print(dne)

export.to_csv("C:/Users/acheung/Desktop/ip_index_testing.csv")

end = time.clock()
elapsed = str(timedelta(seconds=(end - start)))
print("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))
