localhost = True
server = False
morphapikey = '2AMSouHpRtz5kxsTtmUX'
obsprop = {
    "ta_2m":1,
    "rh":2,
    "pa":3,
    "pr_1h":4,
    "ws_avg":5,
    "wd_avg":6
}
if localhost:
    dbhost = '5.189.139.50'
    dbport = '5432'
    dbname = 'sensorthings_sk'
    dbuser = 'sensorthings'
    dbpwd = 'sensorthings'