import datetime
import json

def diff_timestamp_ac(input_ts: str, interval=15):
    """
    :input_ts: string timestamp
    :interval: interval abandoned carts
    return boolean 
    """
    dt_now = datetime.datetime.now()
    dt_event = datetime.datetime.strptime(input_ts, '%Y-%m-%d %H:%M:%S')
    result = int(((dt_now - dt_event).seconds)/60)
    if result >= interval:
        return True
    else:
        return False

def format_output(event):
    result = {'timestamp': event[1].isoformat(' '), 'customer': event[0][0], 'product': event[0][1] }
    return json.dumps(result)
