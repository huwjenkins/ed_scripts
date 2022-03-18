#!/usr/bin/env dials.python
# Attempt to interpret directories containing MRC files and write a datasets.json file
# Author Huw Jenkins 18.03.22
# Last update 18.03.22

from _ctypes import PyObj_FromPtr
import glob
import json
import os
import sys
import re

# modified from https://stackoverflow.com/questions/13249415/how-to-implement-custom-indentation-when-pretty-printing-with-the-json-module
class NoIndent(object):
    """ Value wrapper. """
    def __init__(self, value):
        self.value = value

class CustomEncoder(json.JSONEncoder):
    FORMAT_SPEC = '@@{}@@'
    regex = re.compile(FORMAT_SPEC.format(r'(\d+)'))

    def __init__(self, **kwargs):
      super(CustomEncoder, self).__init__(**kwargs)

    def default(self, obj):
      return (self.FORMAT_SPEC.format(id(obj)) if isinstance(obj, NoIndent) else super(CustomEncoder, self).default(obj))

    def encode(self, obj):
      format_spec = self.FORMAT_SPEC  # Local var to expedite access.
      json_repr = super(CustomEncoder, self).encode(obj)  # Default JSON.
      for match in self.regex.finditer(json_repr):
        # see https://stackoverflow.com/a/15012814/355230
        id = int(match.group(1))
        no_indent = PyObj_FromPtr(id)
        json_obj_repr = json.dumps(no_indent.value)
        json_repr = json_repr.replace('"{}"'.format(format_spec.format(id)), json_obj_repr)
      return json_repr

# default processing options
parameters = {
              "nproc":4,
              "njobs":2,
              "sample":"SAMPLE",
              "spacegroup":None,
              "import":"goniometer.axes=1,0,0 distance=958.5 panel.pedestal=-64",
              "generate_mask":"untrusted.circle='1031 1023 50' untrusted.polygon='0 1010 600 1010 976 1004 988 998 986 1044 978 1041 650 1051 0 1051'",
              "search_beam":True,
              "find_spots":"d_min=1",
              "index":"detector.fix=distance",
              "refine":"detector.fix=distance",
              "integrate":""
}
datasets = []

def make_template(template):
  number, extension = os.path.basename(template).split('.')[-2].split('_')[-1],  template.split('.')[-1]
  return template.replace('.'.join([number,extension]),'.'.join(['####',extension]))

def get_xtal(template):
  xtal = [ x for x in os.path.basename(template).split('_') if 'xtal' in x]
  try:
    return int(xtal[0][4:]) if xtal else None
  except ValueError:
    return None

def get_grid(template):
    grid_idx = [i for i, j in enumerate(template.split(os.path.sep)) if 'grid' in j]
    if len(grid_idx) > 0:
      grid = [g for g in template.split(os.path.sep)[grid_idx[-1]].split('_') if 'grid' in g]
      try:
        return int(grid[0][4:])
      except ValueError:
        return None
    else:
      return None

def generate_datasets(template):
  datasets = []
  templates = glob.glob(os.path.join('**', template),recursive=True)
  for template in templates:
    datasets.append({'template':make_template(template), 
                     'grid':get_grid(template) if get_grid(template) else 1,
                     'xtal':get_xtal(template) if get_xtal(template) else 1})
  return sorted(datasets, key = lambda x: (x['grid'], x['xtal']))

def write_datasets_json(template):
  if not os.path.exists('datasets.json'):
    datasets = generate_datasets(template)
    d = {'parameters':parameters, 'datasets':[NoIndent(ds) for ds in datasets]}
    print(f'Searching for files matching {template} and writing {len(datasets)} dataset(s) to datasets.json')
  else:
    try:
      with open('datasets.json') as f:
        d = json.load(f)
    except json.decoder.JSONDecodeError as e:
      sys.exit(f'datasets.json exists but cannot be read. Error {e}')
    new_datasets = sorted([ds for ds in generate_datasets(template) if ds not in d['datasets']], key = lambda x: (x['grid'], x['xtal']))
    d['datasets'].extend(new_datasets)
    d.update({'datasets':[NoIndent(ds) for ds in d['datasets']]})
    print(f'Searching for files matching {template} and adding {len(new_datasets)} dataset(s) to existing datasets.json')
  with open('datasets.json', 'w') as f:
    print(json.dumps(d, cls=CustomEncoder, indent=2, separators=(',', ':')), file=f)

if __name__ == '__main__':
  if len(sys.argv) != 2:
    sys.exit ('Usage make_datasets_json.py xxx*_0000.mrc')
  else:
    write_datasets_json(sys.argv[1])
