import pymongo
from datetime import datetime
import urllib.request
import json
import sys
sys.path.insert(0, '/home/bneron/Mobyle/mobyle_statistics')

from mobyle_statistics import uniq_user, count_jobs, user_count_per_service, jobs_count_per_service


client = pymongo.MongoClient('localhost', 27017, w=1, j=True)
db_name = 'mobyle_1'
db = client[db_name]
col = db.logs
start = datetime(2014, 1, 1)
stop = datetime(2015, 1, 1)

users = uniq_user(col, start=start, stop=stop)
pasteuriens = uniq_user(col, start=start, stop=stop, pasteurien=True, foreigner=False)
foreigners = uniq_user(col, start=start, stop=stop, pasteurien=False, foreigner=True)

jobs = count_jobs(col, start=start, stop=stop)
jobs_pa = count_jobs(col, start=start, stop=stop, pasteurien=True, foreigner=False)
jobs_fo = count_jobs(col, start=start, stop=stop, pasteurien=False, foreigner=True)

uc = user_count_per_service(col, start=start, stop=stop)
uc_fo = user_count_per_service(col, start=start, stop=stop, foreigner=True, pasteurien=False)
uc_pa = user_count_per_service(col, start=start, stop=stop, foreigner=False, pasteurien=True)

jc = jobs_count_per_service(col, start=start, stop=stop, pasteurien=True, foreigner=True)
jc_fo = jobs_count_per_service(col, start=start, stop=stop, pasteurien=False, foreigner=True)
jc_pa = jobs_count_per_service(col, start=start, stop=stop, pasteurien=True, foreigner=False)


job_count = {}
for p in jc:
    job_count[p['_id']] = {'all': p['count']}
for p in jc_fo:
    if p['_id'] in job_count:
        job_count[p['_id']]['for'] = p['count']
    else:
        job_count[p['_id']] = {'for': p['count']}
for p in jc_pa:
    if p['_id'] in job_count:
        job_count[p['_id']]['past'] = p['count']
    else:
        job_count[p['_id']] = {'past': p['count']}

user_count = {}
for s_name, count in uc.items():
    user_count[s_name] = {'all': len(count)}
for s_name, count in uc_fo.items():
    if s_name in user_count:
        user_count[s_name]['for'] = len(count)
    else:
        user_count[s_name] = {'for': len(count)}

for s_name, count in uc_pa.items():
    if s_name in user_count:
        user_count[s_name]['past'] = len(count)
    else:
        user_count[s_name] = {'past': len(count)}


resp = urllib.request.urlopen("http://mobyle.pasteur.fr/cgi-bin/net_services.py")
if resp.getcode() == 200:
    all_services = json.loads(resp.readline().decode('utf-8'))
    all_services = all_services.keys()
    unused_services = set(all_services) - set(job_count.keys())
    for service_name in unused_services:
        job_count[service_name] = {'all':0, 'for': 0, 'past': 0}

    unused_services = set(all_services) - set(user_count.keys())
    for service_name in unused_services:
        user_count[service_name] = {'all':0 , 'for': 0, 'past': 0}

else:
    print("request to Mobyle failed: {}: {}".format(resp.getcode(), resp.msg), file=sys.stderr)
    print("unused services will not take in account")
#############################

print("Distinct users")
print("pasteuriens, {}".format(len(pasteuriens)))
print("foreigners, {}".format(len(foreigners)))
print("all, {}".format(len(users)))
print()
print("programs + workflows used")
print("pasteuriens, {}".format(len(uc_pa)))
print("foreigners, {}".format(len(uc_fo)))
print("all, {}".format(len(uc)))
print()
print("jobs launched")
print("pasteuriens, {}".format(jobs_pa))
print("foreigners, {}".format(jobs_fo))
print("all, {}".format(jobs))
print()
print("details of jobs launched")
print("service, jobs pasteuriens, jobs foreigners, jobs")

for item in sorted(job_count.items(), key=lambda x: x[1]['all'], reverse=True):
    print("{}, {}, {}, {}".format(item[0],
                                  item[1].get('past') if item[1].get('past') else 0,
                                  item[1].get('for') if item[1].get('for') else 0,
                                  item[1].get('all') if item[1].get('all') else 0
                                 )
          )
print()
print("Distinct users per service")
print("service, jobs pasteuriens, jobs foreigners, jobs")
for item in sorted(user_count.items(), key=lambda x: x[1]['all'], reverse=True):
    print("{}, {}, {}, {}".format(item[0],
                                  item[1].get('past') if item[1].get('past') else 0,
                                  item[1].get('for') if item[1].get('for') else 0,
                                  item[1].get('all') if item[1].get('all') else 0
                                 )
          )
