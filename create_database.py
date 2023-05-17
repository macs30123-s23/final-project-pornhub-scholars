################################################################################
"""
Creates the database tables for the pornhub data hosted on an AWS RDS instance.

Calling this script with flag --create will create the database tables.
Calling this script with flag --close will close the database, shut it down.
Calling this script with flag --download will download the database to the local
    machine as a .parquet file.

"""
################################################################################
import argparse
import boto3
import json
from botocore.exceptions import ClientError
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

def download_database(ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD):
    """
    Download the database as a parquet file.
    """

    with open('db_details.txt', 'r') as f:
        ENDPOINT = f.readline().strip()
        PORT = f.readline().strip()
        rdb_name = f.readline().strip()
        USERNAME = f.readline().strip()
        PASSWORD = f.readline().strip()

    engine = create_engine(f'mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{ENDPOINT}:{PORT}/{rdb_name}')
    
    tables = ['video_info', 'comments', 'creators']
    for table in tables:
        df = pd.read_sql_query(f'SELECT * FROM {table}', engine)
        df.to_parquet(f'{table}.parquet')

def create_aws_rdb():
    """
    Initialize the actual aws rdb instance.
    """
    rdb_name = 'porn_data'

    rds = boto3.client('rds', region_name='us-east-1')

    try:
        response = rds.create_db_instance(
            DBInstanceIdentifier='relational-db',
            DBName=rdb_name,
            MasterUsername='username',
            MasterUserPassword='password',
            DBInstanceClass='db.t2.micro',
            Engine='mysql',
            AllocatedStorage=5
        )
        # Wait until DB is available to continue
        rds.get_waiter('db_instance_available').wait(DBInstanceIdentifier='relational-db')
    except ClientError as e:
        if e.response['Error']['Code'] == 'DBInstanceAlreadyExists':
            print("DB instance already exists. Retrieving its information...")
        else:
            raise

    db = rds.describe_db_instances(DBInstanceIdentifier='relational-db')['DBInstances'][0]
    ENDPOINT = db['Endpoint']['Address']
    PORT = db['Endpoint']['Port']
    DBID = db['DBInstanceIdentifier']

    print(DBID, "\n",
            "is available at\n", ENDPOINT, "\n"
            "on Port\n", PORT,
            )
    USERNAME = 'username'
    PASSWORD = 'password'

    # Get Name of Security Group
    SGNAME = db['VpcSecurityGroups'][0]['VpcSecurityGroupId']
    ec2 = boto3.client('ec2', region_name='us-east-1')
    # Adjust Permissions for that security group so that we can access it on Port 3306
    # If already SG is already adjusted, print this out
    try:
        data = ec2.authorize_security_group_ingress(
                GroupId=SGNAME,
                IpPermissions=[
                    {'IpProtocol': 'tcp',
                    'FromPort': PORT,
                    'ToPort': PORT,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
                    ]
        )
    except ec2.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == 'InvalidPermission.Duplicate':
            print("Permissions already adjusted.")
        else:
            print(e)

    with open('db_details.txt', 'w') as f:
        f.write(f'{ENDPOINT}\n{PORT}\n{rdb_name}\n{USERNAME}\n{PASSWORD}')

    return ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD

def create_database_table(ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD):

    conn =  mysql.connector.connect(host=ENDPOINT,
                                user=USERNAME,
                                passwd=PASSWORD, 
                                port=PORT, 
                                database=rdb_name)
    cursor = conn.cursor(buffered=True)

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS video_info (
                        view_key TEXT PRIMARY KEY,
                        title TEXT,
                        creator_name TEXT,
                        creator_href TEXT,
                        views INTEGER, 
                        rating FLOAT, 
                        year_added TEXT,
                        categories TEXT,
                        timestamp TIMESTAMP
                        )"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS comments ( 
                        username_href TEXT,
                        view_key TEXT NOT NULL,
                        comment_text TEXT,
                        upvotes INTEGER,
                        timestamp TIMESTAMP,
                        FOREIGN KEY (view_key) REFERENCES video_info (view_key))"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS creators (
                    creator_href TEXT PRIMARY KEY,
                    creator_name TEXT,
                    creator_type TEXT,
                    about_info TEXT,
                    video_count INTEGER,
                    subscribers TEXT,
                    infos TEXT,
                    timestamp TIMESTAMP,
                    FOREIGN KEY (creator_href) REFERENCES video_info(creator_href))"""
    )
    conn.commit()
    conn.close()

def delete_database():
    """
    Delete the database.
    """
    rds = boto3.client('rds', region_name='us-east-1')
    response = rds.delete_db_instance(DBInstanceIdentifier='relational-db',
                    SkipFinalSnapshot=True
                    )
    print(response['DBInstance']['DBInstanceStatus'])

    # wait until DB is deleted before proceeding
    rds.get_waiter('db_instance_deleted').wait(DBInstanceIdentifier='relational-db')
    print("RDS Database has been deleted")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Manage the AWS RDS database.')
    parser.add_argument('--create', action='store_true', help='create the database tables')
    parser.add_argument('--close', action='store_true', help='close the database')
    parser.add_argument('--download', action='store_true', help='download the database to the local machine as a .parquet file')
    args = parser.parse_args()
    
    if args.create:
        ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD = create_aws_rdb()
        create_database_table(ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD)
    elif args.close:
        delete_database()
    elif args.download:
        download_database()