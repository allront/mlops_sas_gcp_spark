/*********************************************
*************** Prebuild Job *****************
**********************************************
Program Name : run_build.sas
Owner : ivnard/rusarg that developed this code
Program Description : Runs Python script for
downloading model from Model Manager to server.

**********************************************
**********************************************
**********************************************/

filename logfile '/opt/demos/mlops_sas_gcp_spark/logs/run_build.log';

proc printto log=logfile;
run;

*%global ProjectName;

*For testing;
*%let ProjectName = 'sas_modelops_tensorflow_openshift';
*%put &ProjectName.;

filename bashpipe pipe "/opt/demos/mlops_sas_gcp_spark/run_demo.sh build";

data _null_;
infile bashpipe;
input;
put _infile_;
run;