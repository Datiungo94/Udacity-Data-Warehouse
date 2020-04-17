import configparser
import psycopg2

# CONFIG
config = configparser.ConfigParser()
config.read('config/dwh.cfg')

def connect():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config['CLUSTER']['HOST'],
        config['CLUSTER']['DWH_DB'],
        config['CLUSTER']['DWH_USER'],
        config['CLUSTER']['DWH_PASSWORD'],
        config['CLUSTER']['DWH_PORT'])
    )
    cur = conn.cursor()
    return cur, conn

