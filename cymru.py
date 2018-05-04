import time
from datetime import timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup

start = time.clock()

# sheet = pd.read_excel("C:/Users/acheung/Desktop/JIRA6012_germany.xlsx", sheetname=1)
sheet = pd.read_csv("C:/Users/acheung/Desktop/missed_ranges.csv")
inputs = []

# for i in list(zip(sheet['OCTS(A.START_IP)'], sheet['NGA'])):
#    if i[1] == 'AC':
#        inputs.append(i[0])

for i in sheet['start_octs']:
    inputs.append(i)


results = []
url = 'http://v4.whois.cymru.com/cgi-bin/whois.cgi'
data = {
    'method_whois': 'whois',
    'family': 'ipv4',
    'action': 'do_whois'
}


for input in inputs:
    data['bulk_paste'] = input

    r = requests.post(url=url, data=data)
    bs = BeautifulSoup(r.content, 'html.parser')
    pre_tag = bs.find('pre')
    result = pre_tag.get_text().split("\n")[1:-1]

    if not result:
        result = '--INVALID--'

    result_string = ""
    for r in result:
        result_string += r.replace(',',' ')+","
    results.append({'input': input, 'output': result_string})
    print ("in: {input} out: {output}".format(input=input, output=result_string[:-1]))




# export output CHANGE FILE NAME!
file_name = 'cymru.csv'
f = open(file_name, 'w')

header = "input,name\n"
f.write(header)

for r in results:
    f.write("{input},{name}\n".format(input=r['input'], name=r['output']))

f.close()


print("items:{len}".format(len=len(results)))


end = time.clock()
elapsed = str(timedelta(seconds=(end - start)))
print ("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))
