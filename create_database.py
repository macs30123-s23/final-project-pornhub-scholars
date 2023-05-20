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
from botocore.exceptions import BotoCoreError, ClientError
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import configparser
import os


def write_config(ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD):
    config = configparser.ConfigParser()
    config.read('db_details.ini')
    config["DATABASE"] = {
        "ENDPOINT": ENDPOINT,
        "PORT": PORT,
        "rdb_name": rdb_name,
        "USERNAME": USERNAME,
        "PASSWORD": PASSWORD,
    }

    with open("db_details.ini", "w") as configfile:
        config.write(configfile)


def read_config():
    config = configparser.ConfigParser()
    config.read("db_details.ini")
    ENDPOINT = config.get("DATABASE", "ENDPOINT")
    PORT = config.get("DATABASE", "PORT")
    rdb_name = config.get("DATABASE", "rdb_name")
    USERNAME = config.get("DATABASE", "USERNAME")
    PASSWORD = config.get("DATABASE", "PASSWORD")
    return ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD


def download_database():
    """
    Download the database as a parquet file.
    """

    ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD = read_config()

    engine = create_engine(
        f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{ENDPOINT}:{PORT}/{rdb_name}"
    )

    os.makedirs("./tables", exist_ok=True)

    tables = ["video_info", "comments", "creators"]
    for table in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table}", engine)
        df.to_parquet(f"./tables/{table}.parquet")

def create_s3_bucket():
    """
    Creates the S3 Bucket for the zip file for lambda upload the zip
    """
    s3_client = boto3.client('s3')
    iam_client = boto3.client('iam')
    role = iam_client.get_role(RoleName='LabRole')


    bucket_name = 'final-project-pornhub-macss'
    file_name = 'lambda_deployment.zip'
    key_name = 'lambda_deployment.zip'

    try:
        # Create the S3 bucket
        s3_client.create_bucket(Bucket=bucket_name)

        # Upload the file to S3
        s3_client.upload_file(file_name, bucket_name, key_name)

        # Set the object ACL as public read and write
        s3_client.put_object_acl(
            ACL='public-read-write',
            Bucket=bucket_name,
            Key=key_name
        )
        print(f"File {file_name} uploaded to bucket {bucket_name} and set as public read and write.")
        
    except BotoCoreError as e:
        print(e)
    except ClientError as e:
        print(e)


def create_aws_rdb():
    """
    Initialize the actual aws rdb instance.
    """
    print("Creating AWS RDS Instance...")
    rdb_name = "porn_data"

    rds = boto3.client("rds", region_name="us-east-1")

    try:
        response = rds.create_db_instance(
            DBInstanceIdentifier="relational-db",
            DBName=rdb_name,
            MasterUsername="username",
            MasterUserPassword="password",
            DBInstanceClass="db.t3.medium",
            Engine="mysql",
            AllocatedStorage=10,
        )
        # Wait until DB is available to continue
        print("Waiting for database to be available...")
        rds.get_waiter("db_instance_available").wait(
            DBInstanceIdentifier="relational-db"
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBInstanceAlreadyExists":
            print("DB instance already exists. Retrieving its information...")
        else:
            raise

    db = rds.describe_db_instances(DBInstanceIdentifier="relational-db")["DBInstances"][
        0
    ]
    ENDPOINT = db["Endpoint"]["Address"]
    PORT = db["Endpoint"]["Port"]
    DBID = db["DBInstanceIdentifier"]

    print(
        DBID,
        "\n",
        "is available at\n",
        ENDPOINT,
        "\n" "on Port\n",
        PORT,
    )
    USERNAME = "username"
    PASSWORD = "password"

    # Get Name of Security Group
    SGNAME = db["VpcSecurityGroups"][0]["VpcSecurityGroupId"]
    ec2 = boto3.client("ec2", region_name="us-east-1")
    # Adjust Permissions for that security group so that we can access it on Port 3306
    # If already SG is already adjusted, print this out
    try:
        data = ec2.authorize_security_group_ingress(
            GroupId=SGNAME,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": PORT,
                    "ToPort": PORT,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                }
            ],
        )
    except ec2.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidPermission.Duplicate":
            print("Permissions already adjusted.")
        else:
            print(e)
    print("Permissions adjusted.")
    print("Database is ready to be used.")
    print("Writing database details to config file...")
    write_config(ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD)

    return ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD


def create_database_table(ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD):
    """
    Create the database tables.
    """
    print("Creating database tables...")
    conn = mysql.connector.connect(
        host=ENDPOINT, user=USERNAME, passwd=PASSWORD, port=PORT, database=rdb_name
    )
    cursor = conn.cursor(buffered=True)

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS creators (
                    creator_href VARCHAR(255) PRIMARY KEY,
                    creator_name TEXT,
                    creator_type TEXT,
                    about_info TEXT,
                    video_count INTEGER,
                    subscribers TEXT,
                    infos TEXT,
                    timestamp TIMESTAMP)"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS video_info (
                        view_key VARCHAR(255) PRIMARY KEY,
                        title TEXT,
                        creator_name TEXT,
                        creator_href VARCHAR(255),
                        views INTEGER, 
                        rating FLOAT, 
                        year_added TEXT,
                        categories TEXT,
                        timestamp TIMESTAMP,
                        FOREIGN KEY (creator_href) REFERENCES creators(creator_href))"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS comments ( 
                        username_href TEXT,
                        view_key VARCHAR(255) NOT NULL,
                        comment_text TEXT,
                        upvotes INTEGER,
                        timestamp TIMESTAMP,
                        FOREIGN KEY (view_key) REFERENCES video_info (view_key))"""
    )

    conn.commit()
    conn.close()
    print("Tables created successfully!")


def delete_database():
    """
    Delete the database.
    """
    print("Deleting database...")
    rds = boto3.client("rds", region_name="us-east-1")
    response = rds.delete_db_instance(
        DBInstanceIdentifier="relational-db", SkipFinalSnapshot=True
    )
    print(response["DBInstance"]["DBInstanceStatus"])

    # wait until DB is deleted before proceeding
    rds.get_waiter("db_instance_deleted").wait(DBInstanceIdentifier="relational-db")
    print("RDS Database has been deleted")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage the AWS RDS database.")
    parser.add_argument(
        "--create", action="store_true", help="create the database tables"
    )
    parser.add_argument("--close", action="store_true", help="close the database")
    parser.add_argument(
        "--download",
        action="store_true",
        help="download the database to the local machine as a .parquet file",
    )
    args = parser.parse_args()

    if args.create:
        ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD = create_aws_rdb()
        create_database_table(ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD)
        create_s3_bucket()
    elif args.close:
        delete_database()
    elif args.download:
        download_database()
