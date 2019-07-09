from __future__ import absolute_import

import argparse
import json
import logging
from builtins import object
from dateutil.parser import parse
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from common.functions import diff_timestamp_ac,format_output
from common.logger import setting_log

LOGGER = setting_log(flag_stdout=False, flag_logfile=True)

class BeamJsonCoder(object):
    def encode(self, obj):
        return json.dumps(obj)

    def decode(self, obj):
        return json.loads(obj)


class excludeCheckout(beam.DoFn):
    def process(self, item, excluded_list):
        if (item.get('customer') not in excluded_list[0]) and \
            (diff_timestamp_ac(item.get('timestamp'))):
            yield item

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        required=False,
                        help='Input file to process.',
                        default='./input/*.json')
    parser.add_argument('--output',
                        required=False,
                        help='Output file to write results to.',
                        default='./output/')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        input_dataset = (p 
            | 'read' >> beam.io.ReadFromText(known_args.input, coder=BeamJsonCoder())) 
        
        customers_checkout = beam.pvalue.AsList(input_dataset
            | 'select' >> beam.Map(lambda record: (record.get('page'), record.get('customer')))
            | 'groupBy' >> beam.GroupByKey()
            | 'where_equals_checkout' >> beam.Filter(lambda x: x[0] == 'checkout')
            | 'select_customer' >> beam.Map(lambda x: x[1])
            )

        (input_dataset 
            | 'filter_valid_events' >> beam.ParDo(excludeCheckout(), excluded_list=customers_checkout)
            | 'select_events' >> beam.Map(lambda item: \
                ((item.get('customer'), item.get('product')), parse(item.get('timestamp')))) 
            | 'get_max_ts' >> beam.CombinePerKey(max)
            | 'format_output' >> beam.Map(format_output)
            | 'writing' >> beam.io.WriteToText(known_args.output, '.json')
        )

if __name__ == "__main__":
    run()