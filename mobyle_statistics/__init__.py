#========================
# :Date:Sept 23, 2014
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: BSD
#========================

import sys
import ldap3
from datetime import datetime

#def bypass_email(func):
#    def wrapper(email):
#        return email 
#    return wrapper

def memoize(func):
    cache = {}
    def wrapper(*args):
        if args in cache:
            return cache[args]
        res = func(*args)
        cache[args] = res
        return res
    return wrapper

 
@memoize
def get_long_email(email):
    user, domainName = email.split('@')
    base_dn='ou=personnes,ou=utilisateurs,dc=pasteur,dc=fr'
    if user.find('.') != -1:
        filter = '(& (objectclass=posixAccount) (mail={}))'.format(email)
    else:
        filter = '(& (objectclass=posixAccount) (uid={}))'.format(user)
    attrs =['mail']
    with ldap3.Connection(ldap3.Server('ldap.pasteur.fr', use_ssl=True), auto_bind=True, check_names=True) as con:
        resp = con.search(base_dn, filter, attributes=attrs)
        email = con.response[0]['attributes']['mail'][0] if resp else None
    return email


@memoize
def get_login(email):
    user, domainName  = email.split('@')
    base_dn='dc=pasteur,dc=fr'
    user_base_dn='ou=personnes,ou=utilisateurs,' + base_dn 
    if user.find('.') != -1:
        filter = '(& (objectclass=posixAccount) (mail={}))'.format(email)
    else:
        filter = '(& (objectclass=posixAccount) (uid={}))'.format(user)
    attrs =['uid']
    with ldap3.Connection(ldap3.Server('ldap.pasteur.fr', use_ssl=True), auto_bind=True, check_names=True) as con:
        resp = con.search(base_dn, filter, attributes=attrs)
        email = con.response[1]['attributes']['uid'][0] if resp else None
    return email


def get_unit(login):
    """
    :param login: the login of a pasteurian user
    :type login: string
    :returns: the name of theunit the user belong to
    :rtype: string
    """
    base_dn='dc=pasteur,dc=fr'
    user_base_dn='ou=personnes,ou=utilisateurs,' + base_dn
    filter = '(& (objectclass=posixAccount) (uid={}))'.format(login)
    attrs =['gidNumber']
    server = ldap3.Server('ldap.pasteur.fr', use_ssl=True)
    with ldap3.Connection(server, auto_bind=True, check_names=True) as con:
        resp = con.search(base_dn, filter, attributes=attrs)
        if resp and len(con.response) > 1:
            gid_number = con.response[1]['attributes']['gidNumber'][0]
        else:
            return None
    group_base_dn = 'ou=entites,ou=groupes,' + base_dn
    filter = '(& (objectclass=posixGroup) (gidNumber={}))'.format(gid_number)
    attrs = ['description']
    with ldap3.Connection(server, auto_bind=True, check_names=True) as con:
        resp = con.search(group_base_dn, filter, attributes=attrs)
        unit = con.response[0]['attributes']['description'][0] if resp else None
    return unit
        
        
        
def parse_login_email(login_email_mapping):
    """
    some pateuriens may quit pasteur so they no are anylonger in ldap.
    Then instead of requesting the ldaps, we must request an export of PasteurID.
    This file was generated by Ganael on Sep 22th 2014
    
    :param login_email_mapping: the path to the file of mapping loging<->email
    :type login_email_mapping: string
    """
    login2email = {}
    
    def parse():
        with open(login_email_mapping, 'r') as in_file:
            for line in in_file:
                login, long_email = line.split(';')
                login = login[1:-1]
                # we have to remove the last " and the \n 
                long_email = long_email[1:-2] 
                login2email[login] = long_email
    #fill the login2email dict which will use in the closure
    parse()           
    
    def get_long_email(short_email):
        login = short_email.split('@')[0]
        if login.find('.') == -1:
            try:
                long_email = login2email[login]
            except KeyError:
                long_email = short_email
        else:
            long_email = short_email
        return long_email
    
    email2login = {v:k for k,v in login2email.items()}
    
    def get_login(long_email):
        user = long_email.split('@')[0]
        if user.find('.') == -1:
            login = user
        else:
            login = email2login[long_email]
        return login
    
    return get_long_email, get_login

#get_long_email, get_login = parse_login_email()


def make_log_parser(db_path):
    """
    :param db_path: the path to the maxmind GeoLite2 city database (.mmdb file)
    :type db_path: string
    """
    month_of_year = {'Jan' : 1, 'Feb' : 2, 'Mar' : 3, 'Apr' : 4,
                     'May' : 5, 'Jun' : 6, 'Jul' : 7, 'Aug' : 8,
                     'Sep' : 9, 'Oct' : 10, 'Nov' : 11, 'Dec' : 12}
    
    get_location = get_location_resolver(db_path)
    
    def parse_log(log_file):
        """
        log = {'_id': string,
               'date': datetime,
               'service_name': string,
               'submition_email': string,
               'user': string ,
               'pasteurien: bool,
               'ip': string,
               'from_portal': string
               'submit_from':{continent:,
                              country:
                              city:
                              longitude: float,
                              latitude: float,
                              }
                }
        """
        for line in log_file:
            log = {}
            print(line, end='', file=sys.stderr)
            # 0    1   2   3     4        5         6               7            8           9
            # Sun, 01 Jan 2012 01:33:54 epestfind D01522410861969 why@no.com 129.85.134.129 pasteur
            # beware service_name can contains space thank's user defined workflows
            day_of_week, day_of_month, month, year, timestamp, *service_name, job_id, submition_email, ip, from_portal = line.strip().split()
            if not service_name:
                #this is a pre portal log
                service_name = job_id
                job_id = submition_email
                submition_email = ip
                ip = from_portal
                from_portal = 'pasteur'
            else:
                service_name = ' '.join(service_name)
                
            submition_email = submition_email.lower()
            year = int(year)
            if year == 2010:
                #the mobyle opening was in 2011
                #2010 it was for debugging only
                continue
            month = month_of_year[month]
            day_of_month = int(day_of_month)
            hour, minute, second = map(int, timestamp.split(':'))
            log['date'] = datetime(year, month, day_of_month, hour, minute, second)
            log['service_name'] = service_name
            log['job_id'] = job_id
            if submition_email.endswith('pasteur.fr'):
                log['pasteurien'] = True
                log['user'] = get_long_email(submition_email)
                try:
                    login = get_login(submition_email)
                except KeyError as err:
                    print("WARNING : {0} NOT in PasteurID export".format(submition_email), file=sys.stderr)
                    unit = 'UNKNOWN'
                else:
                    unit = get_unit(login)
                log['unit'] = unit
            else:
                log['pasteurien'] = False
                log['user'] = submition_email
            log['ip'] = ip
            if from_portal == 'unknown':
                from_portal = 'pasteur'
            log['from_portal'] = from_portal
            log['submit_from'] = get_location(log['ip'])
            yield(log)
    return parse_log

        
  
def get_location_resolver(db_path):
    """
    generate a geoip database reader and function get_location
    that use it
    
    :param db_path: the path to the maxmind GeoLite2 city database (.mmdb file)
    :type db_path: string
    :return function get_location
    :rtype: function
    """
    import geoip2.database
    import geoip2.errors
    db_reader = geoip2.database.Reader(db_path)
    
    def get_location(ip):
        """
        :param ip: the internet address to look up
        :type ip: string
        :return: a dictionary containing the location corresponding to the ip
        :rtype: dict {'continent': string,
                      'country': string,
                      'city': string
                      'longitude': float,
                      'latitude': float}
        """
        try:
            response = db_reader.city(ip)
        except geoip2.errors.AddressNotFoundError:
            print("WARNING : IP {} not in DB".format(ip), file=sys.stderr)
            return None
        if response is not None:
            location = {}
            location['continent'] = response.continent.code
            location['country'] = response.country.iso_code
            location['city'] = response.city.name
            location['geoname_id'] = response.city.geoname_id
            location['longitude'] = response.location.longitude
            location['latitude'] = response.location.latitude
            return location
    return get_location



###############
#             #
# count users #
#             #
###############


def uniq_user(col, pasteurien = True, foreigner = True, start = None, stop = None):
    #db.logs.distinct( "user", { date :{"$gte": start , "$lt": end}, pasteurien: true}
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
        
    if pasteurien and not foreigner:
        match['pasteurien'] = True
    elif not pasteurien and foreigner:
        match['pasteurien'] = False
    elif not pasteurien and not foreigner:
        return 0
    else:
        pass
    print("match = ", match, file=sys.stderr)
    res = col.find(match).distinct('user')
    return res


def user_count_per_service(col, pasteurien = True, foreigner = True, start = None, stop = None):
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
        
    if pasteurien and not foreigner:
        match['pasteurien'] = True
    elif not pasteurien and foreigner:
        match['pasteurien'] = False
    elif not pasteurien and not foreigner:
        return 0
    else:
        pass
    
    group = {'_id' : {'service_name' : "$service_name",
                      'user' : "$user"},
             'count' : {'$sum' : 1}
            }
    sort = {'count' : 1}

    pipeline = []
    if match:
        pipeline.append({'$match' : match})
    pipeline.append({'$group' : group})
    pipeline.append({'$sort' : sort})

    res = col.aggregate(pipeline);
    if res['ok'] != 1:
        raise RuntimeError("the request to mongoDB failed")
    else:
        services = {}
        for item in res['result']:
            user = item['_id']['user']
            service_name = item['_id']['service_name']
            count = item['count']
            if service_name in services:
                services[service_name].append((user, count))
            else:
                services[service_name] = [(user, count)]
        return services




##################
#                #
# count services #
#                #
##################
def services_used(col, pasteurien = True, foreigner = True, start = None, stop = None):
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
        
    if pasteurien and not foreigner:
        match['pasteurien'] = True
    elif not pasteurien and foreigner:
        match['pasteurien'] = False
    elif not pasteurien and not foreigner:
        return 0
    else:
        pass
    res = col.find(match).distinct('service_name')
    return res


def services_count_per_user(col, pasteurien = True, foreigner = True, user = None, start = None, stop = None):
    # db.logs.aggregate( [ {$match: { date : {$gte: start, $lt: end }}}, {$group : { _id: {service_name: "$service_name", user:"$user"}, count:{$sum:1} }}, {$sort: {_id:1}}])
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
        
    if pasteurien and not foreigner:
        match['pasteurien'] = True
    elif not pasteurien and foreigner:
        match['pasteurien'] = False
    elif not pasteurien and not foreigner:
        return 0
    else:
        pass
    
    if user is not None:
        match['user'] = user
    
    group = {'_id' : {'service_name' : "$service_name",
                      'user' : "$user"},
             'count' : {'$sum' : 1} 
            }
    
    sort = {'_id' : 1}

    pipeline = []
    if match:
        pipeline.append({'$match' : match})
    pipeline.append({'$group' : group})
    pipeline.append({'$sort' : sort})

    res = col.aggregate(pipeline);
    if res['ok'] != 1:
        raise RuntimeError("the request to mongoDB failed")
    else:
        users = {}
        for item in res['result']:
            user = item['_id']['user']
            service_name = item['_id']['service_name']
            count = item['count']
            if user in users:
                users[user].append((service_name, count))
            else:
                users[user] = [(service_name, count)]
        return users
    
    
############## 
#            #
# count jobs #
#            #
##############


def count_jobs(col, pasteurien = True, foreigner = True, start = None, stop = None):
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
        
    if pasteurien and not foreigner:
        match['pasteurien'] = True
    elif not pasteurien and foreigner:
        match['pasteurien'] = False
    elif not pasteurien and not foreigner:
        return 0
    else:
        pass
    res = col.find(match).count()
    return res



def jobs_count_per_service(col, pasteurien = True, foreigner = True, start = None, stop = None):
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
        
    if pasteurien and not foreigner:
        match['pasteurien'] = True
    elif not pasteurien and foreigner:
        match['pasteurien'] = False
    elif not pasteurien and not foreigner:
        return 0
    else:
        pass
    
    group = {'_id' : "$service_name", 
             'count' : {'$sum' : 1 } 
            }
    sort = {'count' : -1}

    pipeline = []
    if match:
        pipeline.append({'$match' : match})
    pipeline.append({'$group' : group})
    pipeline.append({'$sort' : sort})

    res = col.aggregate(pipeline);
    
    if res['ok'] != 1:
        raise RuntimeError("the request to mongoDB failed")
    else:
        return res['result']


def jobs_count_per_continent(col, start = None, stop = None):
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
            
    group = {'_id' : "$submit_from.continent", 
             'count' : {'$sum' : 1 } 
            }
    sort = {'count' : -1}

    pipeline = []
    if match:
        pipeline.append({'$match' : match})
    pipeline.append({'$group' : group})
    pipeline.append({'$sort' : sort})

    res = col.aggregate(pipeline);
    
    if res['ok'] != 1:
        raise RuntimeError("the request to mongoDB failed")
    else:
        return res['result']


def jobs_count_per_country(col, continent= None, start = None, stop = None):
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
        
    if continent is not None:
        match['submit_from.continent'] = continent
    
    group = {'_id' : "$submit_from.country", 
             'count' : {'$sum' : 1 } 
            }
    sort = {'count' : -1}

    pipeline = []
    if match:
        pipeline.append({'$match' : match})
    pipeline.append({'$group' : group})
    pipeline.append({'$sort' : sort})

    res = col.aggregate(pipeline);
    
    if res['ok'] != 1:
        raise RuntimeError("the request to mongoDB failed")
    else:
        return res['result']
    

def jobs_count_per_city(col, country = None, start = None, stop = None):
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
        
    if country is not None:
        match['submit_from.country'] = country
    
    group = {'_id' : "$submit_from.city", 
             'count' : {'$sum' : 1 } 
            }
    sort = {'count' : -1}
    
    pipeline = []
    if match:
        pipeline.append({'$match' : match})
    pipeline.append({'$group' : group})
    pipeline.append({'$sort' : sort})

    res = col.aggregate(pipeline);
    
    if res['ok'] != 1:
        raise RuntimeError("the request to mongoDB failed")
    else:
        return res['result']


def jobs_count_per_user(col, start = None, stop = None, pasteurien = True, foreigner = True):
    #db.logs.aggregate( [ {$match: { pasteurien : true}}, {$group : { _id: "$user", count:{$sum:1} }}, {$sort: {count:-1}} ]);
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
    
    if pasteurien and not foreigner:
        match['pasteurien'] = True
    elif not pasteurien and foreigner:
        match['pasteurien'] = False
    elif not pasteurien and not foreigner:
        return 0
    else:
        pass
    
    group = {'_id' : "$user", 
             'count' : {'$sum' : 1 } 
            }
    sort = {'count' : -1}
    
    pipeline = []
    pipeline.append({'$match' : match})
    pipeline.append({'$group' : group})
    pipeline.append({'$sort' : sort})

    res = col.aggregate(pipeline);
    
    if res['ok'] != 1:
        raise RuntimeError("the request to mongoDB failed")
    else:
        return res['result']


def jobs_count_per_unit(col, start = None, stop = None):
    #db.logs.aggregate( [ {$match: { pasteurien : true, date : {$gte: start, $lt: end }}}, {$group : { _id: "$unit", count:{$sum:1} }}, {$sort: {count:-1}} ]);
    
    match = {}
    if start and stop:
        match['date'] =  {"$gte": start, "$lt": stop}
    elif start:
        match['date'] =  {"$gte": start}
    elif stop:
        match['date'] =  {"$lt": stop}
    
    match['pasteurien'] = True
    
    group = {'_id' : "$unit", 
             'count' : {'$sum' : 1 } 
            }
    sort = {'count' : -1}
    
    pipeline = []
    pipeline.append({'$match' : match})
    pipeline.append({'$group' : group})
    pipeline.append({'$sort' : sort})

    res = col.aggregate(pipeline);
    
    if res['ok'] != 1:
        raise RuntimeError("the request to mongoDB failed")
    else:
        return res['result']
    
    
    
    
    
    
    
    
    
    
    
#     db.logs.aggregate( [ {$match: { pasteurien : true}}, {$group : { _id: "$unit", count:{$sum:1} }
#                                                           }, {$sort: {count:-1}} ]);
# 
#     db.logs.aggregate( [ {$match: { pasteurien : true, date :{"$gte": start }}}, 
#                      {$group : {
#                                 _id : { "unité" : "$unit"}}, 
#                       jobs: {$sum:1}}])
# 
# 
#     db.logs.aggregate( [ {$match: { pasteurien : true, date :{"$gte": start }}}, 
#                     {$group : { _id : {user: "$user", year: { $year: "$date" }, jobs: {$sum:1}} }, 
#                     {$sort:{jobs:-1}}]
#                      
#                      
#     db.logs.aggregate( [ {$match: {date : {$gte: start, $lt: end }}},
#                      {$group: { _id : {user: "$user", jobs: {$sum:1}}}},
#                 ]);              
#                      
#                      
#     var user  = db.logs.distinct( "user", { date :{"$gte": start , "$lt": end}})
#   > user.length
#  
#     nombre de jobs non Francais              
#     db.runCommand({count:'logs' , query:{date : {$gte: start, $lt: end }, "submit_from.country":{$ne: 'FR'}  }})                            
#   
#                   
#   db.logs.aggregate( [ {$match: { date : {$gte: start, $lt: end }}}, {$group : { _id: "$submit_from.continent", count:{$sum:1} }}, {$sort: {count:-1}} ]);