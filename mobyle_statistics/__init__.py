#========================
# :Date:Sept 23, 2014
# :Authors: Bertrand Néron
# :Contact: bneron<at>pasteur<dot>fr
# :Organization: Institut Pasteur
# :license: BSD
#========================

import sys
#import ldap
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

 
# @memoize
# def get_long_email(email):
#     
#     ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
#     con = ldap.initialize( 'ldaps://ldap.pasteur.fr' )
#     con.simple_bind_s()
#     user, domainName  = email.split('@')
# 
#     base_dn='ou=personnes,ou=utilisateurs,dc=pasteur,dc=fr'
#     if user.find('.') != -1:
#         filter = '(& (objectclass=posixAccount) (mail=%s))'.format(email)
#     else:
#         filter = '(& (objectclass=posixAccount) (uid=%s))'.format(user)
#     attrs =['mail']
#     user = con.search_s(base_dn, ldap.SCOPE_SUBTREE, filter, attrs)
#     if user:
#         email = user[0][1]['mail'][0]
#     return email


def get_unit(login):
     
    ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
    con = ldap.initialize('ldaps://ldap.pasteur.fr')
    con.simple_bind_s()
    base_dn='dc=pasteur,dc=fr'
    
    user_base_dn='ou=personnes,ou=utilisateurs,' + base_dn 
    filter = '(& (objectclass=posixAccount) (uid={}))'.format(login)
    attrs =['gidNumber']
    user = con.search_s(user_base_dn, ldap.SCOPE_SUBTREE, filter, attrs)
    if user:
        gid_number = user[0][1]['gidNumber'][0]
    else:
        return
    goup_base_dn = 'ou=entites,ou=groupes,' + base_dn
    filter = '(& (objectclass=posixGroup) (gidNumber={}))'.format(gid_number)
    attrs = ['description']
    group = con.search_s(goup_base_dn, ldap.SCOPE_SUBTREE, filter, attrs)
    if group:
        try:
            return group[0][1]['description'][0]
        except KeyError:
            return
        
def parse_login_email():
    login2email = {}
    
    def parse():
        with open('/home/bneron/Mobyle/mobyle_statistics/data/mails.txt', 'r') as in_file:
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
        return longin
    
    return get_long_email, get_login

get_long_email, get_login = parse_login_email()


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
            print(line, end='')
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
                login = get_login(submition_email)
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
            print("IP {} not in DB".format(ip), file=sys.stderr)
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


def num_of_user_by_service(col, pasteurien = True, foreigner = True, year = None, service = None):
    pass

def num_of_service_by_users(col, pasteurien = True, foreigner = True, year = None):
    pass

def num_of_jobs_by_service(col, pasteurien = True, foreigner = True, year = None, service = None):
    filters = {}
    if year:
        start = datetime(year, 1, 1)
        end = datetime(year +1, 1, 1)
        filters['date'] =  {"$gte": start, "$lt": end}
    if pasteurien and not foreigner:
        filters['pasteurien'] = True
    elif not pasteurien and foreigner:
        filters['pasteurien'] = False
    elif not pasteurien and not foreigner:
        return 0
    else:
        pass
    if service :
        filters['service_name'] = service
    res = col.find(filters).count()
    return res

def num_of_jobs_by_country(connection, year = None):
    pass

def num_of_jobs_by_city(connection, country = None, year = None):
    pass