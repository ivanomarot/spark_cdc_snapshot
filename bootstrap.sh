#!/bin/bash
# bootstrap.sh is responsible for bootstrapping an EMR cluster
# with relevant Python packages.
# The script can be used through an EMR bootstrap action.

sudo /usr/bin/python3 -m pip install --upgrade pip
sudo /usr/bin/python3 -m pip install boto3==1.16.15 defopt==6.0.1

echo "Success! Bootstrapping complete."