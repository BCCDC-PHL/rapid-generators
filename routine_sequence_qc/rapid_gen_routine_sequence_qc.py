#!/usr/bin/env python3

import argparse
import datetime
import glob
import json
import os
import re
import subprocess


def include_input_dirs(input_dirs, inclusion_criteria):
    """
    Generate a list of existing run directories in the analysis_parent_dir
    Exclude files (not directories) and directories that don't match miseq_run_dir_regex
    input: ["/path/to/runs201228_M00325_0168_000000000-G67AT", ...], {"criterion_name": lambda input_dir: predicate(input_dir), ...}
    output:  ["/path/to/runs/201228_M00325_0168_000000000-G67AT", "/path/to/runs/201229_M04446_0278_000000000-GTF3G", ...]
    """
    selected_input_dirs = []
    for input_dir in input_dirs:
        criteria_met = [inclusion_criterion(input_dir) for _ , inclusion_criterion in inclusion_criteria.items()]
        if all(criteria_met):
            selected_input_dirs.append(input_dir)
        else:
            pass

    return selected_input_dirs


def exclude_input_dirs(input_dirs, exclusion_criteria):
    """
    Remove input dirs that do not meet at least one criterion
    input: ["path/to/runs/201030_M00325_0255_000000000-G5T13", ...], {"criterion_name": lambda input_dir: predicate(input_dir), ...}}
    output: ["path/to/runs/201205_M00325_0282_000000000-G31A8", ...]
    """
    selected_input_dirs = []

    for input_dir in input_dirs:
        criteria_met = [exclusion_criterion(input_dir) for _, exclusion_criterion in exclusion_criteria.items()]
        if any(criteria_met):
            pass
        else:
            selected_input_dirs.append(input_dir)

    return selected_input_dirs


def main(args):

    with open(args.config, 'r') as f:
        pipeline_config = json.load(f)

    instrument_run_dir_regexes = {
        'miseq': '\d{6}_[A-Z0-9]{6}_\d{4}_\d{9}-[A-Z0-9]{5}',
        'nextseq': '\d{6}_[A-Z0-9]{7}_\d+_[A-Z0-9]{9}',
    }

    input_dir_inclusion_criteria = {
        'input_dir_regex_match': lambda input_dir: re.match(instrument_run_dir_regexes['miseq'], os.path.basename(input_dir)) or re.match(instrument_run_dir_regexes['nextseq'], os.path.basename(input_dir)),
        'upload_complete': lambda input_dir: os.path.isfile(os.path.join(input_dir, 'COPY_COMPLETE')),
    }

    input_dir_exclusion_criteria = {
        'output_dir_exists': lambda input_dir: os.path.exists(os.path.join(input_dir, 'RoutineQC')),
        'before_start_date': lambda input_dir: datetime.datetime(int("20" + os.path.basename(input_dir)[0:2]),
                                                                 int(os.path.basename(input_dir)[3:4]),
                                                                 int(os.path.basename(input_dir)[5:6])) < \
        datetime.datetime(int(args.starting_from.split('-')[0]), int(args.starting_from.split('-')[1]), int(args.starting_from.split('-')[2])) 
    }
    
    # Generate list of existing directories in args.analysis_parent_dir
    input_parent_dir_subdirs = list(filter(os.path.isdir, [os.path.join(args.input_parent_dir, f) for f in os.listdir(args.input_parent_dir)]))
    
    candidate_input_dirs = []
    candidate_input_dirs = include_input_dirs(input_parent_dir_subdirs, input_dir_inclusion_criteria)

    # Find runs that haven't already been analyzed
    input_dirs_to_analyze = exclude_input_dirs(candidate_input_dirs, input_dir_exclusion_criteria)

    generate_output_param = lambda x: os.path.join(x['input'], 'RoutineQC')

    for input_dir in input_dirs_to_analyze:
        pipeline_config['pipeline_params']['run_dir'] = os.path.abspath(input_dir)
        pipeline_config['pipeline_params']['outdir'] = os.path.abspath(generate_output_param({"input": input_dir}))
        pipeline_config['pipeline_launch_dir'] = os.path.abspath(input_dir)
        
        print(json.dumps(pipeline_config))


if __name__ == '__main__':    
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input-parent-dir", required=True, help="Parent directory under which input directories are stored")
    parser.add_argument("-c", "--config", required=True, help="JSON-formatted template for pipeline configurations")
    parser.add_argument("-s", "--starting-from", default="1970-01-01", help="Earliest date of run to analyze.")
    args = parser.parse_args()
    main(args)
