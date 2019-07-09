import json

from common.utils import diff_timestamp_ac
import apache_beam as beam

class BeamJsonCoder(object):
    """
    Class for loads json object
    """
    def encode(self, obj):
        return json.dumps(obj)

    def decode(self, obj):
        return json.loads(obj)

class BeamExcludeCheckout(beam.DoFn):
    """
    Class filters events that do not have checkout
    """
    def process(self, item, excluded_list):
        if (item.get('customer') not in excluded_list[0]) and \
            (diff_timestamp_ac(item.get('timestamp'))):
            yield item