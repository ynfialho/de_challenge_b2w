from __future__ import absolute_import

def process_abandoned_carts(argv=None):
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--input',
                            required=False,
                            help='Input file to process, try using the wildcard star character',
                            default='./input/*.json')
        parser.add_argument('--output',
                            required=False,
                            help='Output file to write results to.',
                            default='./output/')
        known_args, pipeline_args = parser.parse_known_args(argv)
        pipeline_options = PipelineOptions(pipeline_args)

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
                | 'filter_valid_events' >> beam.ParDo(BeamExcludeCheckout(), excluded_list=customers_checkout)
                | 'select_events' >> beam.Map(lambda item: ((item.get('customer'), item.get('product')), parse(item.get('timestamp')))) 
                | 'get_max_ts' >> beam.CombinePerKey(max)
                | 'format_output' >> beam.Map(format_output)
                | 'writing' >> beam.io.WriteToText(file_path_prefix=known_args.output\
                    , file_name_suffix='abandoned-carts.json'\
                    , shard_name_template=''\
                    )  
                )
    except Exception as e:
        LOGGER.error(e)      

if __name__ == "__main__":
    import argparse
    import json
    import logging
    from dateutil.parser import parse
    import datetime

    from common.utils import diff_timestamp_ac, format_output
    from common.beam_utils import BeamJsonCoder, BeamExcludeCheckout
    from common.logger import setting_log    
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions

    LOGGER = setting_log(flag_stdout=True, flag_logfile=True)
    process_abandoned_carts()