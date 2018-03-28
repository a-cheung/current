import time
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
import re

start = time.clock()


page = requests.get('http://orgcleanup.quova.com/missing_asn_review.jsp')
soup = BeautifulSoup(page.content, 'html.parser')

asn = []
for i in soup.find_all('input', disabled='disabled'):
    string = str(i)
    value = re.search('value="([0-9]*)"?', string).group(1)
    asnumber = "AS{value}".format(value=value)
    asn.append(asnumber)


results = []
url = 'http://v4.whois.cymru.com/cgi-bin/whois.cgi'
data = {
    'method_whois': 'whois',
    'family': 'ipv4',
    'action': 'do_whois'
}

for a in asn:
    data['bulk_paste'] = a

    r = requests.post(url=url, data=data)
    bs = BeautifulSoup(r.content, 'html.parser')
    pre_tag = bs.find('pre')
    result = pre_tag.get_text().split("\n")[1]

    if not result:
        result = '--INVALID--'

    results.append({'input': a, 'output': result})
    print ("input: {input}  output: {output}".format(input=a, output=result))


print ("items: {n}".format(n=len(results)))


# #####   export csv CHANGE FILE NAME
# file_name = 'C:/Users/acheung/Documents/missing_asn/missing_asn_032118.csv'
# f = open(file_name, 'w')
#
# header = "ASN,name\n"
# f.write(header)
#
# for result in results:
#     row = "{input},{name}\n".format(input=result['input'], name=result['output'])
#     f.write(row)
#
# f.close()


end = time.clock()
elapsed = str(timedelta(seconds=(end - start)))
print ("time elapsed: {elapsed}".format(elapsed=elapsed[:-3]))