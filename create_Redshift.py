import boto3
import pandas as pd
import json
from time import sleep, time
from config import config
from botocore.exceptions import ClientError

def create_iam_read_role(iam, iamrolename):
    # Create a new IAM Role (for read only)
    # Return IAM role ARN
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=iamrolename,
            Description="Allows Redshift clusters to call AWS services on your behalf",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement':
                    [
                        {
                        'Action': 'sts:AssumeRole',
                        'Effect':'Allow',
                        'Principal': 
                            {'Service': 'redshift.amazonaws.com'}
                        }
                    ],
                'Version': '2012-10-17'
                }
            )
        )
    except ClientError as e:
        print ('Error at Creating IAM role:', e)
        
    # Attach role policy
    iam.attach_role_policy(RoleName=iamrolename,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']


def create_cluster(redshift, roleArn):
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=config.DWH_CLUSTER_TYPE,
            NodeType=config.DWH_NODE_TYPE,
            NumberOfNodes=int(config.DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=config.DWH_DB,
            ClusterIdentifier=config.DWH_CLUSTER_IDENTIFIER,
            MasterUsername=config.DWH_USER,
            MasterUserPassword=config.DWH_PASSWORD,
            # Role
            IamRoles=roleArn
        )
    except ClientError as e:
        print('Error at Creating cluster:', e)

    # Get cluster's properties
    # And check if cluster is created
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=config.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    is_cluster_Available = myClusterProps['ClusterStatus'] == 'available'
    timeout = time() + 60 * 15 # Timeout in 15 minutes

    while is_cluster_Available is False:
        sleep(10)
        is_cluster_Available = myClusterProps['ClusterStatus'] == 'available'
        if time() > timeout:
            sys.exit('Failed to create cluster')
    if is_cluster_Available:
         print('Cluster is created.')
    return myClusterProps


def authorize_ingress(ec2, props):
    try:
        vpc = ec2.Vpc(id=props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config.DWH_PORT),
            ToPort=int(config.DWH_PORT)
            )
    except Exception as e:
        print('Error at Authorize ingress:', e)


def main():
    # Create clients
    aws_region = 'us-west-2'
    KEY=config.KEY
    SECRET=config.SECRET

    ec2 = boto3.resource('ec2', region_name=aws_region,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    s3 = boto3.resource('s3', region_name=aws_region,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    iam = boto3.client('iam', region_name=aws_region,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    redshift = boto3.client('redshift', region_name=aws_region,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    # Create IAM role
    create_iam_read_role(iam, config.DWH_IAM_ROLE_NAME)
    roleArn = iam.get_role(RoleName=config.DWH_IAM_ROLE_NAME)['Role']['Arn']

    # Create Redshift cluster
    redshift_props = create_cluster(redshift, [roleArn])
    DWH_ENDPOINT = redshift_props['Endpoint']

    print('DWH_ROLE_ARN :: ', roleArn)
    print('DWH_ENDPOINT :: ', DWH_ENDPOINT)

    # Open an incoming TCP port to access the cluster endpoint
    authorize_ingress(ec2, redshift_props)


if __name__ == '__main__':
    main()