# Gather data from 2 configuration files

import configparser

# Load Master IAM from masterIam.cfg
master_aim_config = configparser.ConfigParser()
master_aim_config.read_file(open('config/masterIAM.cfg'))

KEY = master_aim_config.get('KEY', 'KEYID')
SECRET = master_aim_config.get('KEY', 'SECRET_ACCESS')

# Load data warehouse's configuration from dwh.cfg
dwh_config = configparser.ConfigParser()
dwh_config.read_file(open('config/dwh.cfg'))

[DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_USER, DWH_PASSWORD, DWH_PORT,
    DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE,
    DWH_IAM_ROLE_NAME, HOST, DWH_ROLE_ARN] = [path for key, path in dwh_config.items('CLUSTER')]

# Load source data path
[LOG_DATA, LOG_JSONPATH, SONG_DATA] = [path for key, path in dwh_config.items('S3')]
