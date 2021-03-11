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
        context['input'] = i
        criteria_met = [exclusion_criterion(context) for _, exclusion_criterion in exclusion_criteria.items()]
        if any(criteria_met):
            pass
        else:
            selected_inputs.append(i)

    context.pop('input', None)
    
    return context, selected_inputs


def main(args):

    with open(args.config, 'r') as f:
        message = json.load(f)

    instrument_run_dir_regexes = {
        'miseq': '\d{6}_[A-Z0-9]{6}_\d{4}_\d{9}-[A-Z0-9]{5}',
        'nextseq': '\d{6}_[A-Z0-9]{7}_\d+_[A-Z0-9]{9}',
    }

    input_inclusion_criteria = {
        'input_dir_regex_match': lambda c: re.match(instrument_run_dir_regexes['miseq'], os.path.basename(c['input'])) or re.match(instrument_run_dir_regexes['nextseq'], os.path.basename(c['input'])),
        'upload_complete': lambda c: os.path.isfile(os.path.join(c['input'], 'COPY_COMPLETE')),
    }

    input_exclusion_criteria = {}
    input_exclusion_criteria['output_dir_exists'] = lambda c: os.path.exists(os.path.join(c['input'], 'RoutineQC'))
    if args.after:
        input_exclusion_criteria['before_start_date'] = lambda c: datetime.datetime(int("20" + os.path.basename(c['input'])[0:2]),
                                                                                    int(os.path.basename(c['input'])[3:4]),
                                                                                    int(os.path.basename(c['input'])[5:6])) \
                                                                                    < \
                                                                  datetime.datetime(int(args.after.split('-')[0]),
                                                                                    int(args.after.split('-')[1]),
                                                                                    int(args.after.split('-')[2])) 
    if args.before:
        input_exclusion_criteria['before_start_date'] = lambda c: datetime.datetime(int("20" + os.path.basename(c['input'])[0:2]),
                                                                                    int(os.path.basename(c['input'])[3:4]),
                                                                                    int(os.path.basename(c['input'])[5:6])) \
                                                                                    > \
                                                                  datetime.datetime(int(args.before.split('-')[0]),
                                                                                    int(args.before.split('-')[1]),
                                                                                    int(args.before.split('-')[2]))

    # Generate list of existing directories in args.analysis_parent_dir
    input_subdirs = list(filter(os.path.isdir, [os.path.join(args.input_parent_dir, f) for f in os.listdir(args.input_parent_dir)]))
    
    candidate_inputs = []
    context = {}
    context, candidate_inputs = include_inputs(context, input_subdirs, input_inclusion_criteria)

    # Find runs that haven't already been analyzed
    context, selected_inputs = exclude_inputs(context, candidate_inputs, input_exclusion_criteria)

    generate_output_param = lambda c: os.path.join(c['input'], 'RoutineQC')

    pipeline_name = message['positional_arguments_before_flagged_arguments'][0]

    for i in selected_inputs:
        message_id = str(uuid.uuid4())
        message["message_id"] = message_id
        if 'correlation_id' not in message or not message['correlation_id']:
            correlation_id = str(uuid.uuid4())
            message["correlation_id"] = correlation_id
        message['timestamp_message_created'] = datetime.datetime.now().isoformat()
        message["message_type"] = 'command_creation'
        message['command_invocation_directory'] = os.path.abspath(i)
        this_second_iso8601_str = datetime.datetime.now().strftime('%Y-%m-%dT%H%M%S') 
        today_iso8601_str = this_second_iso8601_str.split('T')[0]
        pipeline_run_id = pipeline_name.replace('/', '_') + "." + message_id

        if '-with-trace' in message['flagged_arguments']:
            trace_dir = os.path.join(message['command_invocation_directory'], "rapid_analysis_logs", "nextflow_traces")
            trace_filename = this_second_iso8601_str + "." + pipeline_run_id + ".trace.txt"
            message['flagged_arguments']['-with-trace'] = os.path.join(trace_dir, trace_filename)

        if '-with-report' in message['flagged_arguments']:
            report_dir = os.path.join(message['command_invocation_directory'], "rapid_analysis_logs", "nextflow_reports")
            report_filename = this_second_iso8601_str + "." + pipeline_run_id + ".report.html"
            message['flagged_arguments']['-with-report'] = os.path.join(report_dir, report_filename)

        if '-work-dir' in message['flagged_arguments']:
            work_dir = os.path.join(message['command_invocation_directory'], "work." + pipeline_run_id)
            message['flagged_arguments']['-work-dir'] = work_dir

        if '--cache' in message['flagged_arguments'] and not message['flagged_arguments']['--cache']:
            message['flagged_arguments']['--cache'] = os.path.expandvars("${HOME}/.conda/envs")
        message['flagged_arguments']['--run_dir'] = os.path.abspath(i)
        message['flagged_arguments']['--outdir'] = os.path.abspath(generate_output_param({"input": i}))
        print(json.dumps(message))

        sentinel = {
            "message_id": str(uuid.uuid4()),
            "correlation_id": correlation_id,
            "message_type": "sentinel",
            "context": {
                "completion_marker_file": os.path.abspath(os.path.join(i, 'RoutineQC', 'analysis_complete.json')),
            }
        }
        print(json.dumps(sentinel))


if __name__ == '__main__':    
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input-parent-dir", required=True, help="Parent directory under which input directories are stored")
    parser.add_argument("-e", "--experiment_name_regex", default="*", help="Regular expression to match in SampleSheet.csv 'Experiment name' field")
    parser.add_argument("-c", "--config", required=True, help="JSON-formatted template for pipeline configurations")
    parser.add_argument("-a", "--after", help="Earliest date of run to analyze.")
    parser.add_argument("-b", "--before", help="Latest date of run to analyze.")
    args = parser.parse_args()
    main(args)
