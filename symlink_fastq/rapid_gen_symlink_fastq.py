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
        context['input'] = os.path.abspath(i)
        context['experiment_name'] = get_experiment_name(os.path.join(context['input'], "SampleSheet.csv"))
        criteria_met = [{criterion_label: bool(inclusion_criterion(context))} for criterion_label, inclusion_criterion in inclusion_criteria.items()]
        if all([list(criterion.values())[0] for criterion in criteria_met]):
            selected_inputs.append(os.path.abspath(i))
        else:
            pass

    context['selected_inputs'] = selected_inputs
    context.pop('input', None)

    return context, selected_inputs


def exclude_inputs(context, inputs, exclusion_criteria):
    """
    Remove input dirs that do not meet at least one criterion
    input: ["path/to/runs/201030_M00325_0255_000000000-G5T13", ...], {"criterion_name": lambda input_dir: predicate(input_dir), ...}}
    output: ["path/to/runs/201205_M00325_0282_000000000-G31A8", ...]
    """
    selected_inputs = []
    for i in inputs:
        context['input'] = os.path.abspath(i)
        criteria_met = [{criterion_label: exclusion_criterion(context)} for criterion_label, exclusion_criterion in exclusion_criteria.items()]
        if any([list(criterion.values())[0] for criterion in criteria_met]):
            context['selected_inputs'] = context['selected_inputs'][1:]
        else:
            selected_inputs.append(i)

    context.pop('input', None)
    context['selected_inputs'] = selected_inputs

    return context, selected_inputs


def get_experiment_name(sample_sheet_path):
    """
    input: "/path/to/SampleSheet.csv"
    output: "20210127-nCoVWGS-98A"
    """
    experiment_name = None
    with open(sample_sheet_path, 'r') as f:
    	for line in f:
            if re.search(r'Experiment Name', line):
                stripped_line = line.strip().rstrip(',')
                experiment_name = stripped_line.split(',')[-1]
    return experiment_name


def main(args):

    with open(args.config, 'r') as f:
        message = json.load(f)
    
    instrument_run_dir_regexes = {
        'miseq': '\d{6}_[A-Z0-9]{6}_\d{4}_\d{9}-[A-Z0-9]{5}',
        'nextseq': '\d{6}_[A-Z0-9]{7}_\d+_[A-Z0-9]{9}',
    }


    input_inclusion_criteria = {
        'input_regex_match': lambda c: re.match(c['instrument_run_dir_regexes']['miseq'], os.path.basename(c['input'])) or re.match(c['instrument_run_dir_regexes']['nextseq'], os.path.basename(c['input'])),
        'upload_complete': lambda c: os.path.exists(os.path.join(c['input'], 'COPY_COMPLETE')) or os.path.exists(os.path.join(c['input'], 'upload_complete.json')),
    }

    input_exclusion_criteria = {}

    rename_fn = lambda f: '_'.join([f[:f.index('.')].split('_')[part] for part in [0, 3]]) + f[f.index('.'):]
    
    # Generate list of existing directories in args.input_parent_dir
    input_subdirs = list(filter(os.path.isdir, [os.path.join(args.input_parent_dir, f) for f in os.listdir(args.input_parent_dir)]))
    
    candidate_inputs = []
    context = {}
    context['instrument_run_dir_regexes'] = instrument_run_dir_regexes

    if args.output_parent_dir:
        context['output'] = args.output_parent_dir
    elif args.output_dir:
        context['output'] = args.output_dir
    context, candidate_inputs = include_inputs(context, input_subdirs, input_inclusion_criteria)

    context, selected_inputs = exclude_inputs(context, candidate_inputs, input_exclusion_criteria)

    generate_destination = lambda c: os.path.join(".", rename_fn(os.path.basename(c['source'])))
    
    for i in selected_inputs:
        correlation_id = str(uuid.uuid4())
        run_id = os.path.basename(i)
        experiment_name = get_experiment_name(os.path.join(i, 'SampleSheet.csv'))
        if args.output_parent_dir:
            message['command_invocation_directory'] = os.path.abspath(os.path.join(args.output_parent_dir, os.path.basename(i)))
        elif args.output_dir:
            message['command_invocation_directory'] = os.path.abspath(args.output_dir)

        if not os.path.exists(message['command_invocation_directory']):
            stashed_message = message
            message = {}
            message["message_id"] = str(uuid.uuid4())
            message["message_type"] = 'command_creation'
            message["correlation_id"] = correlation_id
            message["metadata_context"] = {}
            message["metadata_context"]["run_id"] = run_id
            message["metadata_context"]["experiment_name"] = experiment_name
            message['base_command'] = "mkdir"
            message['flags'] = ["-p"]
            message['positional_arguments'] = [stashed_message['command_invocation_directory']]
            message['command_invocation_directory'] = "."
            message['timestamp_command_created'] = datetime.datetime.now().isoformat()
            print(json.dumps(message))
            message = stashed_message

        miseq_fastq_dir_path = os.path.join("Data", "Intensities", "BaseCalls")
        nextseq_analysis_number = 1
        nextseq_fastq_dir_path = os.path.join("Analysis", str(nextseq_analysis_number), "Data", "fastq")

        fastq_glob = os.path.join(os.path.abspath(i), miseq_fastq_dir_path, "*.fastq.gz")
        fastq_paths = glob.glob(fastq_glob)

        for fastq_path in fastq_paths:
            message["message_id"] = str(uuid.uuid4())
            message["message_type"] = "command_creation"
            message['correlation_id'] = correlation_id
            message["metadata_context"] = {}
            message["metadata_context"]["run_id"] = run_id
            message["metadata_context"]["experiment_name"] = experiment_name
            context['source'] = fastq_path
            message['positional_arguments'] = [fastq_path]

            destination = generate_destination(context)
            message['positional_arguments'].append(destination)
        
            message['timestamp_command_created'] = datetime.datetime.now().isoformat()
            print(json.dumps(message))

        sentinel = {
            "message_id": str(uuid.uuid4()),
            "correlation_id": correlation_id,
            "message_type": "sentinel",
            "context": {
                "completion_marker_file": os.path.abspath(os.path.join(message['command_invocation_directory'], 'symlinks_complete.json')),
            }
        }
        print(json.dumps(sentinel))


if __name__ == '__main__':    
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input-parent-dir", required=True, help="Parent directory under which input directories are stored")
    parser.add_argument("-o", "--output-parent-dir", help="Parent directory under which symlinks will be created")
    parser.add_argument("--output-dir", help="Directory in which symlinks will be created")
    parser.add_argument("-c", "--config", required=True, help="JSON-formatted template for pipeline configurations")
    parser.add_argument("-b", "--before", default="1970-01-01", help="Earliest date of run to analyze.")
    parser.add_argument("-a", "--after", default="1970-01-01", help="Earliest date of run to analyze.")
    args = parser.parse_args()
    main(args)
