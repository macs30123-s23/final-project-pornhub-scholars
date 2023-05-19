# Shell script to run the scraper from zero state
# FROM THE TOP, RUN IT BACK

#! WARNINGS BEFORE RUNNING
#! YOU MUST HAVE YOUR CREDENTIALS SETUP FROM AWS CLI
#! Needed files, check that the .zip file and lambda_function.py are in the root directory
#! You must have the following packages installed: boto3, requests, bs4, pandas, numpy, json,
#!  zipfile, configparser, argparse, os, argparse, os, shutil

# initialize the database
python create_database.py --create
python scrape.py -l 1000 -n 10 --update
