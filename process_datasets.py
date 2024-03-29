#!/usr/bin/env dials.python
# Process multiple microED datasets. 
# Author Huw Jenkins 10.05.21
# Last update 28.07.22

import math
import os
import sys
import time
import logging
import json
from libtbx import easy_run, easy_mp, Auto
from dials.array_family import flex
from dxtbx.serialize import load
from dxtbx.util import format_float_with_standard_uncertainty

__version__ = '0.3.5'

class ProcessDataset:
  def __init__(self, parameters):
      self.parameters = parameters
      self.log = logging.getLogger(__name__)
  def __call__(self, dataset):
    work_dir = os.path.join(os.getcwd(), self.parameters['sample'], f'grid{dataset["grid"]}', f'xtal{dataset["xtal"]:03}')
    dataset_id = '/'.join(work_dir.split(os.path.sep)[-3:])
    try:
      os.makedirs(work_dir)
    except FileExistsError:
      pass
    os.chdir(work_dir)

    # already run?
    if os.path.isfile('integrated.expt'):
      self.log.info(f'Skipping {dataset_id} as file integrated.expt exists')
      return self.get_result(dataset_id=dataset_id, experiments='integrated.expt', reflections='indexed.refl', skipped=True)

    # import
    if dataset.get('template'):
      cmd = f'dials.import template=../../../{dataset["template"]} {self.parameters["import"]}'
    else:
      cmd = f'dials.import ../../../{dataset["file"]} {self.parameters["import"]}'
    if dataset.get('image_range'):
      cmd += f' image_range={dataset["image_range"]}'
    r = easy_run.fully_buffered(command=cmd)
    if len(r.stderr_lines) > 0:
      with open('dials.import.err', 'w') as f:
        f.write('\n'.join(r.stderr_lines))
        if dataset.get('template'):
          self.log.info(f'import of {dataset["template"]} failed!')
        else:
          self.log.info(f'import of {dataset["file"]} failed!')
      return

    if self.parameters.get('generate_mask') and self.parameters['generate_mask'] != '':
      # generate mask
      cmd = f'dials.generate_mask imported.expt {self.parameters["generate_mask"]}'
      r = easy_run.fully_buffered(command=cmd)
      if len(r.stderr_lines) > 0:
        with open('dials.generate_mask.err', 'w') as f:
          f.write('\n'.join(r.stderr_lines))

      # apply mask
      cmd = f'dials.apply_mask imported.expt mask=pixels.mask'
      r = easy_run.fully_buffered(command=cmd)
      if len(r.stderr_lines) > 0:
        with open('dials.apply_mask.err', 'w') as f:
          f.write('\n'.join(r.stderr_lines))
      else:
        expt = 'masked.expt'
    else:
      expt = 'imported.expt'

    # find spots
    cmd = f'dials.find_spots {expt} {self.parameters["find_spots"]} nproc={self.parameters["nproc"]}'
    r = easy_run.fully_buffered(command=cmd)
    if len(r.stderr_lines) > 0:
      with open('dials.find_spots.err', 'w') as f:
        f.write('\n'.join(r.stderr_lines))

    # search beam position
    if self.parameters.get('search_beam'):
      cmd = f'dials.search_beam_position {expt} strong.refl output.experiments=optimised_beam.expt'
      r = easy_run.fully_buffered(command=cmd)
      if len(r.stderr_lines) > 0:
        with open('dials.search_beam_position.err', 'w') as f:
          f.write('\n'.join(r.stderr_lines))
      else:
        expt = 'optimised_beam.expt'

    # find rotation axis
    if self.parameters.get('find_rotation_axis'):
      cmd = f'dials.find_rotation_axis {expt} strong.refl {self.parameters["find_rotation_axis"]} output.experiments=optimised_axis.expt'
      r = easy_run.fully_buffered(command=cmd)
      if len(r.stderr_lines) > 0:
        with open('find_rotation_axis.err', 'w') as f:
          f.write('\n'.join(r.stderr_lines))
      else:
        expt = 'optimised_axis.expt'

    if self.parameters['spacegroup'] in [None, 'P1']:
      # index in P1
      cmd = f'dials.index {expt} strong.refl {self.parameters["index"]}'
      r = easy_run.fully_buffered(command=cmd)
      if len(r.stderr_lines) > 0:
        with open('dials.index.err', 'w') as f:
          f.write('\n'.join(r.stderr_lines))
    else:
      if self.parameters.get('initial_index_P1'):
        cmd = f'dials.index {expt} strong.refl {self.parameters["index"]} output.experiments=P1.expt output.reflections=P1.refl output.log=dials.index_P1.log'
        r = easy_run.fully_buffered(command=cmd)
        if len(r.stderr_lines) > 0:
          with open('dials.index_P1.err', 'w') as f:
            f.write('\n'.join(r.stderr_lines))
        else:
          expt = 'P1.expt'

      cmd = f'dials.index {expt} strong.refl {self.parameters["index"]} space_group={self.parameters["spacegroup"]}'
      r = easy_run.fully_buffered(command=cmd)
      if len(r.stderr_lines) > 0:
        with open('dials.index.err', 'w') as f:
          f.write('\n'.join(r.stderr_lines))
    if not os.path.isfile('indexed.expt'):
      self.log.info(f'{dataset_id} failed to index in space group {self.parameters["spacegroup"]}')
      return

    # refine (static)
    cmd = f'dials.refine indexed.expt indexed.refl {self.parameters["refine"]} scan_varying=false output.experiments=refined_static.expt output.reflections=refined_static.refl'
    r = easy_run.fully_buffered(command=cmd)
    if len(r.stderr_lines) > 0:
      with open('dials.refine_static.err', 'w') as f:
        f.write('\n'.join(r.stderr_lines))
    if not os.path.isfile('refined_static.expt'):
      self.log.info(f'{dataset_id} failed in intitial refinement')
      return

    # refine (scan varying)
    cmd = f'dials.refine refined_static.expt refined_static.refl {self.parameters["refine"]} scan_varying=true'
    r = easy_run.fully_buffered(command=cmd)
    if len(r.stderr_lines) > 0:
      with open('dials.refine.err', 'w') as f:
        f.write('\n'.join(r.stderr_lines))
    if not os.path.isfile('refined.expt'):
      self.log.info(f'{dataset_id} failed in scan varying refinement')
      return

    # integrate 
    cmd = f'dials.integrate refined.expt refined.refl {self.parameters["integrate"]} nproc={self.parameters["nproc"]}'
    r = easy_run.fully_buffered(command=cmd)
    if len(r.stderr_lines) > 0:
      with open('dials.integrate.err', 'w') as f:
        f.write('\n'.join(r.stderr_lines))
    if not os.path.isfile('integrated.expt'):
      self.log.info(f'{dataset_id} failed in integration')
      return

    # success
    return self.get_result(dataset_id=dataset_id, experiments='integrated.expt', reflections='indexed.refl')

  def get_result(self, dataset_id, experiments, reflections, skipped=False):
    el = load.experiment_list(experiments)
    xtal = el.crystals()[0]
    uc = xtal.get_unit_cell().parameters()
    uc_sd = xtal.get_cell_parameter_sd()
    sg = str(xtal.get_space_group().info())
    if not skipped:
      unit_cell = [format_float_with_standard_uncertainty(v, e, minimum=1.0e-5) for (v, e) in zip(uc, uc_sd)]
      self.log.info(f'{dataset_id} processed successfully {sg} {" ".join(unit_cell)}')
    formatted_unit_cell = [f'{v:6.2f}' if e > 1.0e-5 else f'{round(v,0):3.0f}' for (v, e) in zip(uc, uc_sd)]
    refl = flex.reflection_table.from_file(reflections)
    n_total = len(refl)
    refined = refl.get_flags(refl.flags.used_in_refinement)
    indexed = refl.get_flags(refl.flags.indexed)
    n_indexed = len(refl.select(indexed))
    refl = refl.select(refined)
    xo, yo, zo = refl["xyzobs.px.value"].parts()
    xc, yc, zc = refl["xyzcal.px"].parts()
    rmsd_x = math.sqrt(flex.mean(flex.pow2(xo - xc)))
    rmsd_y = math.sqrt(flex.mean(flex.pow2(yo - yc)))
    rmsd_z = math.sqrt(flex.mean(flex.pow2(zo - zc)))
    formatted_rmsds = f'{rmsd_x:8.5f} {rmsd_y:8.5f} {rmsd_z:8.5f}'
    return {'dataset_id':dataset_id,
            'output_files':[os.path.abspath('integrated.expt'), os.path.abspath('integrated.refl')] if not skipped else [],
            'sg':sg,
            'uc':formatted_unit_cell,
            'total':n_total,
            'indexed':n_indexed,
            'unindexed':n_total - n_indexed,
            'rmsds':formatted_rmsds,
           }

def run(datasets_json):
  # start logging to stdout
  log = logging.getLogger()
  log.setLevel(logging.INFO)
  fmt = logging.Formatter('%(message)s')
  ch = logging.StreamHandler(sys.stdout)
  ch.setFormatter(fmt)
  log.addHandler(ch)
  # and logfile
  logfile = 'process_datasets.log'
  logfile = os.path.abspath(logfile)
  if os.path.isfile(logfile):
    os.unlink(logfile)
  log.info(f'Writing logfile to {logfile}')
  fh = logging.FileHandler(logfile)
  fh.setFormatter(fmt)
  log.addHandler(fh)
  log.info(f'Multi 3DED/microED dataset processor version {__version__}')
  log.info(f'Start time: {str(time.asctime(time.localtime(time.time())))}')
  # read datasets.json:
  try:
    with open(datasets_json) as f:
      datasets = json.load(f)
  except json.decoder.JSONDecodeError as e:
    sys.exit(f'Unable to read {datasets_json} Error {e}')
  
  for dataset in datasets['datasets']:
    if dataset.get('template'):
      log.info(f'Processing dataset {dataset["template"]} as {datasets["parameters"]["sample"]}/grid{dataset["grid"]}/xtal{dataset["xtal"]:03}')
    else:
      log.info(f'Processing dataset {dataset["file"]} as {datasets["parameters"]["sample"]}/grid{dataset["grid"]}/xtal{dataset["xtal"]:03}')
  process_dataset = ProcessDataset(datasets['parameters'])
  results = easy_mp.parallel_map(process_dataset,
                                iterable=datasets['datasets'],
                                processes = datasets['parameters']['njobs'],
                                preserve_order=True
                               )
  log.info('Created the following integrated files:')
  successful_results = [r for r in results if r is not None]
  for r in successful_results:
    if len(r['output_files']) > 0:
      log.info(f"{r['output_files'][0]} {r['output_files'][1]}")
  log.info('Summary of results:')
  for r in successful_results:
    log.info(f"{r['dataset_id']} {r['sg']} {' '.join(r['uc'])} {r['indexed']:5d} {r['unindexed']:5d} {100*r['indexed']/r['total']:5.1f} {r['rmsds']}")
  log.info(f'End time: {str(time.asctime(time.localtime(time.time())))}')
if __name__ == '__main__':
  if len(sys.argv) == 1:
    sys.exit('Usage process_datasets datasets.json')
  datasets_json = sys.argv[1]
  if not os.path.isfile(datasets_json):
    sys.exit(f'File {datasets_json} does not exist')
  run(datasets_json=datasets_json)
