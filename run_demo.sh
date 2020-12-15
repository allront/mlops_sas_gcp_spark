#!/usr/bin/env bash

# run_demo.sh
# run_demo.sh is a bash wrapper to execute build and migrate python scripts using SAS Workflow Manager
#
# Variables:
# PROCESS_STEP=${1} can be build or migrate. Remember all sub arguments you have for both commands
#
# Author: Ivan Nardini (ivan.nardini@sas.com)

# Variables
PROCESS_STEP=${1}

# From SAS Viya
VENV=./env/bin/activate

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
   echo "run_demo.sh is a bash wrapper to execute build and migrate python scripts using SAS Workflow Manager"
   echo
   echo "Syntax"
   echo "run_demo.sh build [--project-name <name>| --requirements <filename>| --train-script <filename>| --configfile <filename>| --score-script <filename>]"
   echo "options:"
   echo "--project-name     Your project name on SAS Model Manager"
   echo "--requirements     requirements file of your project"
   echo "--train-script     train filename"
   echo "--configfile       config filename"
   echo "--score-script     score filename"
   echo ""
   echo "run_demo.sh migrate [--bucket-name <name>]"
   echo "--bucket-name      bucket name on Google Cloud Platform"
   echo ""
   exit 0
fi

echo "$(date '+%x %r') INFO Execute run_demo.py"
# On Viya server, for Job execution
cd cd /opt/demos/mlops_sas_gcp_spark/
source ${VENV}
#sudo chmod +x ./run_demo.py
python run_demo.py ${PROCESS_STEP}