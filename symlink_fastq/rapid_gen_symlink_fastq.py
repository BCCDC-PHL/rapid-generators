#!/usr/bin/env python3

import argparse
import datetime
import glob
import json
import os
import re
import subprocess
import uuid


def include_inputs(context, inputs, inclusion_criteria):
    """
    Generate a list of existing run directories in the analysis_parent_dir
    Exclude files (not directories) and directories that don't match miseq_run_dir_regex
    input: ["/path/to/runs201228_M00325_0168_000000000-G67AT", ...], {"criterion_name": lambda input_dir: predicate(input_dir), ...}
    output:  ["/path/to/runs/201228_M00325_0168_000000000-G67AT", "/path/to/runs/201229_M04446_0278_000000000-GTF3G", ...]
    """
    selected_inputs = []
    for i in inputs:
        context['input'] = i
        criteria_met = [inclusion_criterion(context) for _ , inclusion_criterion in inclusion_criteria.items()]
        if all(criteria_met):
            selected_inputs.append(i)
        else:
            pass

    return context, selected_inputs


def exclude_inputs(context, inputs, exclusion_criteria):
    """
    Remove input dirs that do not meet at least one criterion
    input: ["path/to/runs/201030_M00325_0255_000000000-G5T13", ...], {"criterion_name": lambda input_dir: predicate(input_dir), ...}}
    output: ["path/to/runs/201205_M00325_0282_000000000-G31A8", ...]
    """
    
    selected_inputs = []
    for i in inputs:
        context['input'] = i
        criteria_met = [exclusion_criterion(context) for _, exclusion_criterion in exclusion_criteria.items()]
        if any(criteria_met):
            pass
        else:
            selected_inputs.append(i)

    return context, selected_inputs


def main(args):

    with open(args.config, 'r') as f:
        pipeline_config = json.load(f)

    instrument_run_dir_regexes = {
        'miseq': '\d{6}_[A-Z0-9]{6}_\d{4}_\d{9}-[A-Z0-9]{5}',
        'nextseq': '\d{6}_[A-Z0-9]{7}_\d+_[A-Z0-9]{9}',
    }

    input_inclusion_criteria = {
        'input_dir_regex_match': lambda c: re.match(instrument_run_dir_regexes['miseq'], os.path.basename(c['input'])) or re.match(instrument_run_dir_regexes['nextseq'], os.path.basename(c['input'])),
        'upload_complete': lambda c: os.path.isfile(os.path.join(c['input'], 'COPY_COMPLETE')),
    }

    input_exclusion_criteria = {
        'output_dir_exists': lambda c: os.path.exists(os.path.join(c['input'], 'RoutineQC')),
        'before_start_date': lambda c: datetime.datetime(int("20" + os.path.basename(c['input'])[0:2]),
                                                                 int(os.path.basename(c['input'])[3:4]),
                                                                 int(os.path.basename(c['input'])[5:6])) < \
        datetime.datetime(int(args.starting_from.split('-')[0]), int(args.starting_from.split('-')[1]), int(args.starting_from.split('-')[2])) 
    }
    
    # Generate list of existing directories in args.input_parent_dir
    input_subdirs = list(filter(os.path.isdir, [os.path.join(args.input_parent_dir, f) for f in os.listdir(args.input_parent_dir)]))
    
    candidate_inputs = []
    context = {}
    context, candidate_inputs = include_inputs(context, input_subdirs, input_inclusion_criteria)

    # Find runs that haven't already been analyzed
    context, selected_inputs = exclude_inputs(context, candidate_inputs, input_exclusion_criteria)

    generate_output_param = lambda x: os.path.join(x['input'], 'RoutineQC')

    for i in selected_inputs:
        command_id = str(uuid.uuid4())
        command_config['command_id'] = command_id
        command_config['command_invocation_directory'] = os.path.abspath(input_dir)
        this_second_iso8601_str = datetime.datetime.now().strftime('%Y-%m-%dT%H%M%S') 
        today_iso8601_str = this_second_iso8601_str.split('T')[0]

        miseq_fastq_dir_path = os.path.join("Data", "Intensities", "BaseCalls")
        nextseq_analysis_number = 1
        nextseq_fastq_dir_path = os.path.join("Analysis", str(nextseq_analysis_number), "Data", "fastq")

        fastq_glob = os.path.join(os.path.abspath(i), miseq_fastq_dir_path, "*.fastq.gz")

        if 'positional_arguments' in command_config and command_config['positional_arguments']:
            command_config['positional_arguments'].append(fastq_glob)
        else:
            command_config['positional_arguments'] = [fastq_glob]

        positional_arguments.append(".")
        
        command_config['timestamp_command_created'] = datetime.datetime.now().isoformat()
        print(json.dumps(command_config))


if __name__ == '__main__':    
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input-parent-dir", required=True, help="Parent directory under which input directories are stored")
    parser.add_argument("-o", "--output-parent-dir", help="Parent directory under which input directories are stored")
    parser.add_argument("--output-dir", help="Parent directory under which input directories are stored")
    parser.add_argument("-c", "--config", required=True, help="JSON-formatted template for pipeline configurations")
    parser.add_argument("-s", "--starting-from", default="1970-01-01", help="Earliest date of run to analyze.")
    args = parser.parse_args()
    main(args)
