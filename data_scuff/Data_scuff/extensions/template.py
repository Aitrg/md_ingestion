'''
Created on 22-Jul-2018

@author: srinivasan
'''
import os
import string
from jinja2 import Environment


def render_templatefile(path, **kwargs):
    with open(path, 'rb') as fp:
        raw = fp.read().decode('utf8')
    content = Environment(lstrip_blocks=True, trim_blocks=True).from_string(string.Template(raw).substitute(**kwargs)).render(
        {'val_header':kwargs['val_header'],
         'default_val':kwargs['default_val'],
         'ingestion_timestamp':kwargs['ingestion_timestamp'],
         'feed_expo':kwargs['feed_expo']})
    render_path = path[:-len('.tmpl')] if path.endswith('.tmpl') else path
    with open(render_path, 'wb') as fp:
        fp.write(content.encode('utf8'))
    if path.endswith('.tmpl'):
        os.remove(path)
