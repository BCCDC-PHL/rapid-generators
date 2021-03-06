#!/usr/bin/env python3

import argparse
import datetime
import glob
import json
import os
import re
import subprocess
import uuid


def include_inputs(inputs, inclusion_criteria):
    """
    Generate a list of existing run directories in the analysis_parent_dir
    Exclude files (not directories) and directories that don't match miseq_run_dir_regex
    input: ["/path/to/runs201228_M00325_0168_000000000-G67AT", ...], {"criterion_name": lambda input_dir: predicate(input_dir), ...}
    output:  ["/path/to/runs/201228_M00325_0168_000000000-G67AT", "/path/to/runs/201229_M04446_0278_000000000-GTF3G", ...]
    """
    selected_inputs = []
    for i in inputs:
        criteria_met = [inclusion_criterion(i) for _ , inclusion_criterion in inclusion_criteria.items()]
        if all(criteria_met):
            selected_input_dirs.append(i)
        else:
            pass

    return selected_inputs


def exclude_inputs(inputs, exclusion_criteria):
    """
    Remove input dirs that do not meet at least one criterion
    input: ["path/to/runs/201030_M00325_0255_000000000-G5T13", ...], {"criterion_name": lambda input_dir: predicate(input_dir), ...}}
    output: ["path/to/runs/201205_M00325_0282_000000000-G31A8", ...]
    """
    selected_inputs = []

    for i in inputs:
        criteria_met = [exclusion_criterion(i) for _, exclusion_criterion in exclusion_criteria.items()]
        if any(criteria_met):
            pass
        else:
            selected_inputs.append(i)

    return selected_inputs


def main(args):

    with open(args.config, 'r') as f:
        pipeline_config = json.load(f)


    input_inclusion_criteria = {
        'prerequisites_complete': lambda x: True 
    }

    input_exclusion_criteria = {
        'duplicate_analysis': lambda x: False
    }
    
    candidate_inputs = []
    candidate_inputs = include_inputs(candidate_inputs, input_inclusion_criteria)

    # Find runs that haven't already been analyzed
    selected_inputs = exclude_inputs(candidate_inputs, input_exclusion_criteria)

    flagged_param_generators = {
        "--output": "output",
    }

    pipeline_name = pipeline_config['positional_arguments_before_flagged_arguments'][0]

    for i in selected_inputs:
        command_id = str(uuid.uuid4())
        pipeline_config['command_id'] = command_id
        pipeline_config['command_invocation_directory'] = "."
        this_second_iso8601_str = datetime.datetime.now().strftime('%Y-%m-%dT%H%M%S') 
        today_iso8601_str = this_second_iso8601_str.split('T')[0]
        pipeline_run_id = pipeline_name.replace('/', '_') + "." + command_id

        if '-with-trace' in pipeline_config['flagged_arguments']:
            trace_dir = os.path.join(pipeline_config['command_invocation_directory'], "rapid_analysis_logs", "nextflow_traces")
            trace_filename = this_second_iso8601_str + "." + pipeline_run_id + ".trace.txt"
            pipeline_config['flagged_arguments']['-with-trace'] = os.path.join(trace_dir, trace_filename)

        if '-with-report' in pipeline_config['flagged_arguments']:
            report_dir = os.path.join(pipeline_config['command_invocation_directory'], "rapid_analysis_logs", "nextflow_reports")
            report_filename = this_second_iso8601_str + "." + pipeline_run_id + ".report.html"
            pipeline_config['flagged_arguments']['-with-report'] = os.path.join(report_dir, report_filename)

        if '-work-dir' in pipeline_config['flagged_arguments']:
            work_dir = os.path.join(pipeline_config['command_invocation_directory'], "work." + pipeline_run_id)
            pipeline_config['flagged_arguments']['-work-dir'] = work_dir

        if '--cache' in pipeline_config['flagged_arguments'] and not pipeline_config['flagged_arguments']['--cache']:
            pipeline_config['flagged_arguments']['--cache'] = os.path.expandvars("${HOME}/.conda/envs")

        # Additional flagged args...

        pipeline_config['timestamp_command_created'] = datetime.datetime.now().isoformat()
        print(json.dumps(pipeline_config))


if __name__ == '__main__':    
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input", required=True, help="Input")
    parser.add_argument("-c", "--config", required=True, help="JSON-formatted template for pipeline configurations")
    args = parser.parse_args()
    main(args)
