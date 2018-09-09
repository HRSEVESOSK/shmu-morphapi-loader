import json,requests,config,pgsql,argparse
from datetime import datetime,timedelta
morph_api_url = "https://api.morph.io/soit-sk/scraper-shmu-observations/data.json"
morph_api_key = config.morphapikey
ap = argparse.ArgumentParser()
ap.add_argument("-begin",required=False,dest="b",help="Query observations FROM date YYYY-MM-DD")
ap.add_argument("-end",required=False,dest="e",help="Query observations TO date YYYY-MM-DD")
args = vars(ap.parse_args())
YESTERDAY = (datetime.today() - timedelta(6)).isoformat()
if args['b'] and args['e']:
    QUERY = "SELECT * FROM data WHERE date >= '%s' AND date <= '%s' ORDER BY date DESC" % (args['b'], args['e'])
else:
    QUERY = "SELECT * FROM data WHERE date >= '%s' ORDER BY date DESC" % YESTERDAY
OBSPROP = json.loads(json.dumps(config.obsprop))
r = requests.get(morph_api_url, params={
  'key': morph_api_key,
  'query': QUERY
})
results = r.json()
if len(results) > 0:
    print("DATA COLLECTTION PROCESS START ON %s FOR %s OBSERVATIONS" % (datetime.now().isoformat(),len(results)))
    connection = pgsql.PGSql()
    for data in results:
        THGNAME = data['name']
        print(THGNAME)
        RESTIME = data['date']
        print(RESTIME)
        for k,v in data.iteritems():
            if k in OBSPROP:
                sql = """Select  ds."ID", ds."DESCRIPTION", ds."THING_ID" FROM "DATASTREAMS" ds, "OBS_PROPERTIES" op, "THINGS" thg 
                            WHERE ds."OBS_PROPERTY_ID" = op."ID" AND thg."NAME" = '%s ' AND ds."OBS_PROPERTY_ID" = %s AND ds."THING_ID" = thg."ID";""" % (THGNAME,OBSPROP[k])
                connection.connect()
                DSID = connection.query(sql, False)
                connection.close()
                if DSID:
                    # COMPARE RESULT_TIME_START IN DB WITH DATE FROM API
                    sql = """select "ID" from "%s" 
                              where "DATASTREAM_ID" = %s and "RESULT_TIME" = '%s'""" % (config.obstable,DSID[0][0],RESTIME) # to_char("RESULT_TIME"::timestamptz at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as "LAST_DATE"
                    connection.connect()
                    OID = connection.query(sql, False)
                    connection.close()
                    if len(OID) > 1:
                        print("%s - WARNING: '%s' DUPLICATES TO DATASTREAM_ID '%s' AND RESULT TIME '%s'" % (datetime.now().isoformat(),len(OID)-1,DSID[0][0],RESTIME))
                        continue
                    elif len(OID) == 1:
                        print("%s - INFO: DATASTREAM_ID '%s' AND RESULT TIME '%s' ALREADY STORED IN DB WITH OID '%s'" % (datetime.now().isoformat(),DSID[0][0], RESTIME,OID[0][0]))
                        continue
                    else:
                    #if datetime.strptime(RESTIME, '%Y-%m-%dT%H:%M:%SZ') != datetime.strptime(LASTDATE[0][0], '%Y-%m-%dT%H:%M:%SZ'):  # 2018-09-01T21:00:00Z)
                        if v is not None:
                            print("%s - INFO: INSERTING NEW OBSERVATION VALUE '%s' FOR PROPERTY_ID '%s' WITH TIMESTAMP '%s' FOR DS ID '%s' DS DESCRIPTION '%s'" % (datetime.now().isoformat(),v,OBSPROP[k], RESTIME, DSID[0][0], DSID[0][1]))
                            sql = """insert into "%s"("RESULT_TIME","RESULT_NUMBER","DATASTREAM_ID","FEATURE_ID") VALUES ('%s',%s,%s,%s) RETURNING "ID" """ % (config.obstable,RESTIME,v,DSID[0][0],DSID[0][2])
                            connection.connect()
                            OBSID = connection.query(sql, False)
                            connection.close()
                            if OBSID[0][0] > 0:
                                print("%s - INFO: INSERT SUCCESS. OBSERVATION ID: %s" % (datetime.now().isoformat(),OBSID[0][0]))
                        else:
                            print("%s - INFO: PARAMETER '%s' HAS VALUE: '%s'" % (datetime.now().isoformat(),k,v))
                else:
                    print("%s - WARNING: THING NAME '%s' HAS NOT OBSERVED PROPERTY '%s' " % (datetime.now().isoformat(), THGNAME, OBSPROP[k]))
    print("DATA COLLECTTION PROCESS END ON %s" % datetime.now().isoformat())