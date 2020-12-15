/*********************************************
*************** Prebuild Job *****************
**********************************************
Program Name : run_migrate.sas
Owner : ivnard/rusarg that developed this code
Program Description : Runs Python script for
moving model from the server to GCP.

**********************************************
**********************************************
**********************************************/

filename logfile '/opt/demos/mlops_sas_gcp_spark/logs/run_migrate.log';

proc printto log=logfile;
run;

*%global ProjectName;

*For testing;
*%let ProjectName = 'sas_modelops_tensorflow_openshift';
*%put &ProjectName.;

filename bashpipe pipe "/opt/demos/mlops_sas_gcp_spark/run_demo.sh migrate";

data _null_;
infile bashpipe;
input;
put _infile_;
run;