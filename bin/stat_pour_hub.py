
__author__ = 'bneron'


import os
import sys
import glob
import time
from lxml import etree
from abc import ABCMeta, abstractmethod


class Node(metaclass=ABCMeta):

    def __init__(self, name, job=None):
        self.name = name
        self.parent = None
        self.children = {}
        self._job = job if job is not None else {}

    def add_child(self, child):
        child.parent = self
        self.children[child.name] = child
        if child.job:
            self.update_job()
        return child

    @property
    def job(self):
        return self._job

    def __getitem__(self, name):
        return self.children[name]


    @abstractmethod
    def update_job(self):
        pass

    def to_html(self):
        s = '<div data-role="collapsible">\n'
        s += '<h1><span style="color:blue">{name}</span> pasteur:{pasteur:d} | other:{other:d} | total:{all:d}</h1>\n'.format(name=self.name, **self.job)
        for child_name in sorted(self.children):
            s += self.children[child_name].to_html()
        s += '</div>\n'
        return s


class Category(Node):


    def update_job(self):
        pasteur = sum([c.job['pasteur'] if 'pasteur' in c.job else 0 for c in self.children.values()])
        other = sum([c.job['other'] if 'other' in c.job else 0 for c in self.children.values()])
        all_ = sum([c.job['all'] if 'all' in c.job else 0 for c in self.children.values()])
        self._job = {'pasteur': pasteur,
                     'other': other,
                     'all': all_}
        self.parent.update_job()



class Interface(Node):

    def __init__(self, name, job=None, package=None, authors=None, citations=None, homepage=None):
        super().__init__(name, job=job)
        self.parent = []
        self.package = package
        self.authors = authors if authors is not None else []
        self.citations = citations if citations is not None else []
        self.homepage = homepage

    def to_html(self):
        s = '<p><span style="color:blue">{name}</span> pasteur:{pasteur:d} | other:{other:d} | total:{all:d}</p>\n'.format(name=self.name, **self.job)
        return s

    def update_job(self):
        for one_parent in self.parent:
            one_parent.update_job()



class Mobyle(Node):

    def __init__(self):
        super().__init__('Mobyle')

    def update_job(self):
        pasteur = sum([c.job['pasteur'] if 'pasteur' in c.job else 0 for c in self.children.values()])
        other = sum([c.job['other'] if 'other' in c.job else 0 for c in self.children.values()])
        all_ = sum([c.job['all'] if 'all' in c.job else 0 for c in self.children.values()])
        self._job = {'pasteur': pasteur,
                     'other': other,
                     'all': all_}


    def add_interface(self, name, authors, citations, package, homepage, job, categories):

        interface = Interface(name, job=job)
        # retrieve categories
        # if category does not exists yet
        # build and add it
        for cat in categories:
            path = cat.split(':')
            node = self
            for elt in path:
                if elt in node.children:
                    node = node.children[elt]
                else:
                    node = node.add_child(Category(elt))

            # add new interface to the right category
            # one interface can be child of categories
            node.add_child(interface)


    def scan_services(self, repository_path, job_counter):
        interfaces = glob.glob(os.path.join(repository_path, '*.xml'))
        parser = etree.XMLParser(no_network=False)
        for interface in interfaces:
            print("-------- process {} --------".format(os.path.basename(interface)[:-4]), file=sys.stderr)
            doc = etree.parse(interface, parser)
            root = doc.getroot()
            head_node = root.find('./head')
            name = head_node.find('./name').text
            categories = [n.text for n in head_node.findall('./category')]
            authors = None
            citations = None

            package = head_node.find('package/name')
            if package is not None:
                package = package.text

            homepage = None
            job_count = {'pasteur': 0,
                         'other': 0,
                         'all': 0}
            try:
                job_count.update(job_counter[name])
            except KeyError:
                continue
            self.add_interface(name, authors, citations, package, homepage, job_count, categories)


    def to_html(self, stat_start, stat_stop):
        s = """<!DOCTYPE html>
<html>
<head>
<link rel="stylesheet" href="http://code.jquery.com/mobile/1.4.5/jquery.mobile-1.4.5.min.css">
<script src="http://code.jquery.com/jquery-1.11.3.min.js"></script>
<script src="http://code.jquery.com/mobile/1.4.5/jquery.mobile-1.4.5.min.js"></script>
</head>
<body>

<div data-role="page" id="pageone">
  <div data-role="header">
    <h1>Interfaces Mobyle</h1>
  </div>

  <div data-role="main" class="ui-content">"""
        s += super().to_html()
        s += """</div>

  <div data-role="footer">
    <h3> generated the {date}    based on statistics from {start} to {stop}</h3>
  </div>
</div>

</body>
</html>""".format(date=time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime()),
                  start=start,
                  stop=stop)
        return s



if __name__ == '__main__':
    from datetime import datetime
    import pymongo

    sys.path.insert(0, '/home/bneron/Mobyle/mobyle_statistics')
    from mobyle_statistics import jobs_count_per_service

    client = pymongo.MongoClient('localhost', 27017, w=1, j=True)
    db_name = 'mobyle_1'
    db = client[db_name]
    col = db.logs
    start = datetime(2014, 1, 1)
    stop = datetime(2015, 1, 1)

    jc = jobs_count_per_service(col, start=start, stop=stop, pasteurien=True, foreigner=True)
    jc_fo = jobs_count_per_service(col, start=start, stop=stop, pasteurien=False, foreigner=True)
    jc_pa = jobs_count_per_service(col, start=start, stop=stop, pasteurien=True, foreigner=False)

    job_counter = {}
    for p in jc:
        job_counter[p['_id']] = {'all': p['count']}
    for p in jc_fo:
        if p['_id'] in job_counter:
            job_counter[p['_id']]['other'] = p['count']
        else:
            job_counter[p['_id']] = {'other': p['count']}
    for p in jc_pa:
        if p['_id'] in job_counter:
            job_counter[p['_id']]['pasteur'] = p['count']
        else:
            job_counter[p['_id']] = {'pasteur': p['count']}


    mobyle = Mobyle()
    repository_path = '/home/bneron/Mobyle/pasteur-programs/trunk/'
    mobyle.scan_services(repository_path, job_counter)

    with open('mobyle_statistics.html', 'w') as mob_html:
        mob_html.write(mobyle.to_html(start, stop))
