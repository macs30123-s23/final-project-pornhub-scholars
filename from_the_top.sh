#!/bin/bash

# Shell script to run the scraper from zero state
# FROM THE TOP, RUN IT BACK

# If you do not have micromamba python environment manager installed,
# add the --mamba flag from the command below

# install requirements
if [[ $* == *--mamba* ]]
then
    echo "Installing micromamba..."
    curl micro.mamba.pm/install.sh | bash
fi

# create the environment
echo "Creating the environment..."
mamba env create -f environment.yml

# # initialize the database
mamba run -n prn python create_database.py --create
mamba run -n prn python scrape.py -l 1000 -n 20 --update

# Now you have a bunch of data
# Time to analyze it

# When you're done though remember to shut down the RDB so you're not losing money
# run the following command to shut down the RDB
# mamba run -n prn python create_database.py --close
