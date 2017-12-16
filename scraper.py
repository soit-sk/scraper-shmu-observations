#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2017, Peter Hanecak <hanecak@opendata.sk>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import csv
import datetime
import httplib2
import os
import scraperwiki
import time
from StringIO import StringIO


REQUEST_URL = 'http://meteo.shmu.sk/customer/home/opendata/?observations;date=' # + 'DD.MM.YYYY:HH'

COPY_PERIOD = 2 * 365	# two years, roughly

#KEEP_CSV_COPY = False
# FIXME: make sure to turn it off for runs o Morph.io
KEEP_CSV_COPY = True
CSV_COPY_FN_PATTERN = './shmu-observations-%Y%m/shmu-observations-%Y%m%d-%H.csv'

VAR_LATEST_OBSERVATION = 'latest_observation'

# for now hack: a global variable used to throttle amount of requests
MIN_TIME_BETWEEN_REQUESTS = datetime.timedelta(seconds=1)
last_request_time = datetime.datetime.utcnow() \
    - MIN_TIME_BETWEEN_REQUESTS


def get_one(shmu_datetime):
    """
    Just get a CSV from SHMU for specified date and time.
    
    shmu_datetime: DD.MM.YYYY:HH (example: '16.12.2017:11')
    
    returns CSV data
    """
    
    global last_request_time
    
    # throttling fail-safe, i.e.  make at most one request per
    # MIN_TIME_BETWEEN_REQUESTS
    delta = datetime.datetime.utcnow() - last_request_time
    sleep_time = (last_request_time + MIN_TIME_BETWEEN_REQUESTS) \
        - datetime.datetime.utcnow()
    if (sleep_time > datetime.timedelta(seconds=0)):
        print("### making requests too quickly => going to sleep (%s)"
            % sleep_time)
        # FIXME: crude quick hack :/
        st_in_micro = sleep_time.seconds * 1000000
        st_in_micro += sleep_time.microseconds
        time.sleep(sleep_time.total_seconds())
    last_request_time = datetime.datetime.utcnow()
    
    # get the adta
    request_url = REQUEST_URL + shmu_datetime
    print("### getting %s ..." % request_url)
    h = httplib2.Http(".cache")
    (resp_headers, content) = h.request(request_url, "GET")
    data = content.decode()
    
    # write a CSV file, for local copy (maybe also Git preservation)
    if (KEEP_CSV_COPY):
        # file name
        timestamp = datetime.datetime.strptime(shmu_datetime, '%d.%m.%Y:%H')
        fn = timestamp.strftime(CSV_COPY_FN_PATTERN)
        # prepare directory
        pn = os.path.dirname(os.path.abspath(fn))
        if not os.path.exists(pn):
            print("### creating %s" % pn)
            os.makedirs(pn)
        # write
        print("### writing copy of data into %s" % fn)
        f = open(fn, 'w')
        f.write(data)
        f.close()
    
    return data


def fix_value(value):
    """
    Missing values are repesened by "null" - we need to convert those to None.
    
    SHMU is also pretty priting CSV, so we need to strip leading and
    trailing white space from values.
    """
    
    svalue = value.strip()
    if (svalue == "null"):
        return None
    
    return svalue


def process_one(shmu_datetime):
    """
    Get a CSV with observations for specified date and time and put it into
    scrapper DB.

    shmu_datetime: same as 'get_one()' above
    
    returns nothing so far
    """
    
    # get the data
    data = get_one(shmu_datetime)
    
    # parse the CSV
    f = StringIO(data)
    reader = csv.reader(f, delimiter=';')
    
    # store data from CSV into SQLite
    item_count = 0
    for row in reader:
        # skip header
        if (row[1] == 'cccc'):
            continue

        scraperwiki.sqlite.save(
            unique_keys = ['obs_stn', 'date'],
            data = {
                'obs_stn':	row[0],
                'cccc':		row[1].strip(),
                'name':		row[2].strip(),
                'lat':		row[3],
                'lon':		row[4],
                'elev':		row[5].strip(),
                'date':		row[6],
                'ta_2m':	fix_value(row[7]),
                'pa':		fix_value(row[8]),
                'rh':		fix_value(row[9]),
                'pr_1h':	fix_value(row[10]),
                'ws_avg':	fix_value(row[11]),
                'wd_avg':	fix_value(row[12]),
                'scrap_time': datetime.datetime.utcnow().replace(microsecond=0).isoformat()
            }
        )
        item_count += 1
    
    f.close()
    print("### processed %d items for %s" % (item_count, shmu_datetime))
    
    return


def process_whole(period):
    """
    Make a 1:1 copy of SHMU data into lcoal SQLite DB, going back 'period'
    days.
    
    period: how many days to go back
    
    retuns nothing so far
    """
    
    # SHMU seems to have two year history and timestamps in UTC.  We're not
    # going to use anything bellow seconds, so whatever is there, does not
    # matter much.
    scrap_end = datetime.datetime.utcnow() \
        - datetime.timedelta(minutes=10)
        # let's say they are able to collect the measurements in less then 10 minuts
    scrap_start = scrap_end \
        - datetime.timedelta(days=period)
    
    # determine where we've ended up last time running and start from there
    latest_observation = scraperwiki.sqlite.get_var(VAR_LATEST_OBSERVATION)
    if latest_observation is not None:
        scrap_start = datetime.datetime.strptime(latest_observation, "%Y-%m-%dT%H:%M:%S")
        print("### resuming download from %s" % scrap_start)
        # This means we'll donwload the latest observation once again.
        # Let's call that safety margin.

    print("### going to download data from %s to %s" % (scrap_start, scrap_end))
    item_count = 0
    scrap_current = scrap_start
    while (scrap_current <= scrap_end):
        # iterate from 00 to 23 for a day (if the day is "today", make sure to not go beyond current hour)
        shmu_datetime = scrap_current.strftime('%d.%m.%Y:%H')
        process_one(shmu_datetime)
        item_count += 1
        
        # remember what we've already scrapped, so that we do not download it again on next run
        scraperwiki.sqlite.save_var(VAR_LATEST_OBSERVATION,
            scrap_current.replace(microsecond=0).isoformat())

        scrap_current += datetime.timedelta(hours=1)
    
    print("### done: %d observations downloaded" % item_count)


process_whole(COPY_PERIOD)
    