import time
import datetime
from dateutil.parser import parse


class Timestamp:
    def __init__(self, start_time):
        self.start_time = start_time
        pass

    def get_timestamp(self, time_string, granularity):
        start = parse(self.start_time)
        current = parse(time_string)
        timestamp = time.mktime(current.timetuple()) - time.mktime(start.timetuple())  # in second
        if granularity == 'sec':
            return int(timestamp)
        elif granularity == 'min':
            return int(timestamp / 60)
        elif granularity == 'hour':
            return int(timestamp / 3600)
        else:
            print 'The granularity can be sec, min, or hour!'
            return 0

    '''
    Get the weekday and hour for an input timestamp
    Weekday is a number from 1 to 7
    '''
    def get_day_hour(self, timestamp, granularity):
        struct_time = self._to_struct_time(timestamp, granularity)
        print struct_time
        weekday = struct_time.isoweekday()
        hour = struct_time.hour
        return weekday, hour

    def _to_struct_time(self, timestamp, granularity):
        start = parse(self.start_time)
        # the default input granularity is in second
        if granularity == 'sec':
            delta = timestamp
        elif granularity == 'hour':
            delta *= 60
        elif granularity == 'hour':
            delta *= 3600
        else:
            print 'The input timestamp can be sec, min, or hour!'
        current_timestamp = time.mktime(start.timetuple()) + delta
        return datetime.datetime.fromtimestamp(current_timestamp)

if __name__ == '__main__':
    print 'hello'
    t = Timestamp('Wed May 26 23:50:00 2015')
    # ts = t.get_timestamp('2013-02-08 04:00:00', 'sec')
    # print t.get_day_hour(ts, 'sec')
    # t = Timestamp('%A %m %d %X %Z %Y', 'Wed May 27 18:45:49 +0000 2015')
    print t.get_timestamp('Wed May 27 18:45:49 2015', 'sec')
    print t.get_day_hour(0, 'sec')
