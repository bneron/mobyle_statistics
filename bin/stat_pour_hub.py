
__author__ = 'bneron'


import os
import sys
import glob
from lxml import etree


class Node:

    def __init__(self, name, children=None, parent=None, job=None):
        self.name = name
        self.children = children if children is not None else {}
        self.parent = parent
        self._job = job if job is not None else {}

    def _add_node(self, name, job=0):
        child = Node(name, parent=self, job=job)
        self.children[name] = child
        if job:
            self.update_job()
        return child

    def update_job(self):
        pasteur = sum([c.job['pasteur'] if 'pasteur' in c.job else 0 for c in self.children.values()])
        other = sum([c.job['other'] if 'other' in c.job else 0 for c in self.children.values()])
        all_ = sum([c.job['all'] if 'all' in c.job else 0 for c in self.children.values()])
        self._job = {'pasteur': pasteur,
                     'other': other,
                     'all': all_}
        if self.parent:
            self.parent.update_job()

    @property
    def job(self):
        return self._job

    def __getitem__(self, name):
        return self.children[name]

    def to_html(self):
        if self.children:
            s = '<div data-role="collapsible">\n'
            s += '<h1><span style="color:blue">{name}</span> pasteur:{pasteur:d} | other:{other:d} | total:{all:d}</h1>\n'.format(name=self.name, **self.job)
            for child_name in sorted(self.children):
                s += self.children[child_name].to_html()
            s += '</div>\n'
        else:
            s = '<p><span style="color:blue">{name}</span> pasteur:{pasteur:d} | other:{other:d} | total:{all:d}</p>\n'.format(name=self.name, **self.job)
        return s


class Mobyle(Node):

    def __init__(self):
        super().__init__('Mobyle')

    def add_interface(self, name, job, categories):
        for cat in categories:
            path = cat.split(':')
            child = self
            for elt in path:
                if elt in child.children:
                    child = child.children[elt]
                else:
                    child = child._add_node(elt)
            child._add_node(name, job=job)

    def scan_services(self, repository_path, job_counter):
        interfaces = glob.glob(os.path.join(repository_path, '*.xml'))
        parser = etree.XMLParser(no_network=False)
        for interface in interfaces:
            print("-------- process {} --------".format(os.path.basename(interface)[:-4]), file=sys.stderr)
            doc = etree.parse(interface, parser)
            root = doc.getroot()
            head = root.find('./head')
            name = head.find('./name').text
            categories = [n.text for n in head.findall('./category')]
            job_count = {'pasteur': 0,
                         'other': 0,
                         'all': 0}
            job_count.update(job_counter[name])
            self.add_interface(name, job_count, categories)

    def to_html(self):
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
    <h1>Insert Footer Text Here</h1>
  </div>
</div>

</body>
</html>"""

        return s
if __name__ == '__main__':
    # from datetime import datetime
    # import pymongo
    #
    # sys.path.insert(0, '/home/bneron/Mobyle/mobyle_statistics')
    # from mobyle_statistics import jobs_count_per_service
    #
    # client = pymongo.MongoClient('localhost', 27017, w=1, j=True)
    # db_name = 'mobyle_1'
    # db = client[db_name]
    # col = db.logs
    # start = datetime(2014, 1, 1)
    # stop = datetime(2015, 1, 1)
    #
    # jc = jobs_count_per_service(col, start=start, stop=stop, pasteurien=True, foreigner=True)
    # jc_fo = jobs_count_per_service(col, start=start, stop=stop, pasteurien=False, foreigner=True)
    # jc_pa = jobs_count_per_service(col, start=start, stop=stop, pasteurien=True, foreigner=False)
    #
    # job_counter = {}
    # for p in jc:
    #     job_counter[p['_id']] = {'total': p['count']}
    # for p in jc_fo:
    #     if p['_id'] in job_counter:
    #         job_counter[p['_id']]['other'] = p['count']
    #     else:
    #         job_counter[p['_id']] = {'other': p['count']}
    # for p in jc_pa:
    #     if p['_id'] in job_counter:
    #         job_counter[p['_id']]['pasteur'] = p['count']
    #     else:
    #         job_counter[p['_id']] = {'pasteur': p['count']}
    #
    #
    # mobyle = Mobyle()
    # repository_path = '/home/bneron/Mobyle/pasteur-programs/trunk/'
    # mobyle.scan_services(repository_path, job_counter)

    import pickle
    with open('mobyle.dump', 'rb') as dump_file:
        mobyle = pickle.load(dump_file)

    #print("================== clustalw ===========")
    #print(mobyle['alignment']['multiple']['clustalw-multialign'].job)
    #print(mobyle['alignment']['multiple']['clustalw-multialign'])
    #print(mobyle['alignment']['multiple'])

    #print("================== HMM ===========")
    #print(mobyle['hmm'])
    #print("================== Mobyle ===========")
    #print(mobyle)
    #print(mobyle['assembly'].to_html())
    print(mobyle.to_html())
