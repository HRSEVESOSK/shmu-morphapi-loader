import json,requests,config,pgsql
from datetime import datetime,timedelta
morph_api_url = "https://api.morph.io/soit-sk/scraper-shmu-observations/data.json"
morph_api_key = config.morphapikey
YESTERDAY = (datetime.today() - timedelta(1)).isoformat()
OBSPROP = json.loads(json.dumps(config.obsprop))

QUERY = "SELECT * FROM data WHERE date >= '%s' ORDER BY date DESC" % YESTERDAY
r = requests.get(morph_api_url, params={
  'key': morph_api_key,
  'query': QUERY
})
results = r.json()
if len(results) > 0:
    print("DATA COLLECTTION PROCESS START ON %s" % datetime.now().isoformat())
    connection = pgsql.PGSql()
    for data in results:
        THGNAME = data['name']
        RESTIME = data['date']
        for k,v in data.iteritems():
            if k in OBSPROP:
                sql = """Select  ds."ID", ds."DESCRIPTION", ds."THING_ID" FROM "DATASTREAMS" ds, "OBS_PROPERTIES" op, "THINGS" thg 
                            WHERE ds."OBS_PROPERTY_ID" = op."ID" AND thg."NAME" = '%s ' AND ds."OBS_PROPERTY_ID" = %s AND ds."THING_ID" = thg."ID";""" % (THGNAME,OBSPROP[k])
                connection.connect()
                DSID = connection.query(sql, False)
                connection.close()
                if DSID:
                    # GET LAST INSERT DATE
                    sql = """select "RESULT_TIME"::timestamp as "LAST_DATE" from "OBSERVATIONS2" where "DATASTREAM_ID" = %s order by "RESULT_TIME"::date DESC limit 1""" % DSID[0][0]
                    connection.connect()
                    LASTDATE = connection.query(sql, False)
                    connection.close()
                    if datetime.strptime(RESTIME, '%Y-%m-%dT%H:%M:%SZ') > LASTDATE[0][0]:  # 2018-09-01T21:00:00Z
                        print("%s - INFO: INSERTING NEW OBSERVATION VALUE '%s' FOR PROPERTY_ID '%s' WITH TIMESTAMP '%s' FOR DS ID '%s' DS DESCRIPTION '%s'" % (datetime.now().isoformat(),v,OBSPROP[k], RESTIME, DSID[0][0], DSID[0][1]))
                        sql = """insert into "OBSERVATIONS2"("RESULT_TIME","RESULT_NUMBER","DATASTREAM_ID","FEATURE_ID") VALUES ('%s',%s,%s,%s) RETURNING "ID" """ % (RESTIME,v,DSID[0][0],DSID[0][2])
                        connection.connect()
                        OBSID = connection.query(sql, False)
                        connection.close()
                        if OBSID[0][0] > 0:
                            print("INSERT SUCCESS. OBSERVATION ID: %s" % OBSID[0][0])
                            exit()
                    else:
                        print("%s - INFO: RECENT DATA ALREADY STORED IN DB FOR DS ID %s" % (datetime.now().isoformat(), DSID[0][0]))
                else:
                    print("%s - WARNING: THING NAME '%s' HAS NOT OBSERVED PROPERTY '%s' " % (datetime.now().isoformat(), THGNAME, OBSPROP[k]))
    print("DATA COLLECTTION PROCESS END ON %s" % datetime.now().isoformat())