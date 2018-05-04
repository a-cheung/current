import time
from datetime import timedelta
import pandas as pd

start = time.clock()

#INPUT FILE PATH
file_name = "C:/Users/acheung/Desktop/country_improvements/brazil/brazil_full.csv"

df = pd.read_csv(file_name).drop_duplicates(subset='a.gps_start_ip')
header = df['a.gps_start_ip']!='a.gps_start_ip'
df2 = df[header]

file_export = "{input}_PRE.csv".format(input=file_name[:-4])
f = open(file_export, 'w')
f.write("ip,ip_int\n")


for d in df2['a.gps_start_ip']:
    ip_int = int(d)
    ip_oct = '.'.join([str(ip_int >> (i << 3) & 0xFF)
                    for i in range(4)[::-1]])
    f.write("{oct},{int}\n".format(oct=ip_oct,int=ip_int))

f.close()


end = time.clock()
elapsed = str(timedelta(seconds=(end - start)))
print ("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))



