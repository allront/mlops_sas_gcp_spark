# Migrating existing analytics Spark workloads on Google Cloud Platform with SAS 

Nowadays migrating on-premise existing BigData workloads on the cloud is one of the main
customer's challenges.

Based on our experience, it is really complicated for several reasons. 
For example, Data Scientists does not know what is needed to allow their models 
running in the cloud. Or IT and Business is struggling to define a organization process
in respect of compliance and governance.

With this project, we want to show how SAS can support migrating analytics on 
Google Cloud Platform providing business process and governance by SAS Model Manager 
and SAS Workflow Manager

## Overview 

Below the on-premise application architecture of the solution:

<p align="center">
<img src="https://github.com/IvanNardini/mlops_sas_gcp_spark/raw/master/mm_gcp.png">
</p>

## Scenario Description

In as-is: 

0. Data Scientist submits pyspark job to Hadoop-Spark cluster on premise. 

Then migration starts. Then: 

1. Data Scientist creates several pyspark model packages. And he registers different versions in SAS® Model Manager 
using [SAS® sasctl](https://github.com/sassoftware/python-sasctl).

2. Big Data Engineer maps on-prem cluster configuration and create a templates. Then DevOps engineer
may create a cloud function to deploy the cluster (and submit jobs) on Google Cloud Platform

Then, the workflow starts and

3. Each actor involved in the process claims and completes user tasks.
In this case, a Validator approve the Champion model. And an IT provides
Cloud bucket name for migration. 

## Installation

### Server FS side

1. Clone the project.

2. Create virtual environment 

    ```bash
    cd mlops_sas_gcp_spark/
    python3 -m venv env
    source env/bin/activate
    pip install --file requirements.txt
    source deactivate
    ```
3. Create a folder for Viya logs

    ```bash
    mkdir logs
    ```

4. Change group ownership based on SAS Workflow Administration group.
For example, if you have SAS Workflow Administration group called CASHostAccountRequired, 
run 

    ```bash
    chown -R sas:CASHostAccountRequired mlops_sas_gcp_spark/
    ```
### SAS Viya side

1. Import [ModelOps for GCP](src/workflow/definition/) in SAS Workflow Manager

2. Import [run_build.sas](src/workflow/definition/run_build.sas) and [run_migrate.sas](src/workflow/definition/run_migrate.sas)
jobs in SAS Job Execution

**Notice: Don't forget to double checks RESTAPIs endpoints in the workflow definition service tasks and 
paths in the jobs**

## Usage

From SAS Model Manager project, start ModelOps for GCP workflow and user tasks one by one.

Below some frames of the demo.

## Contributing
Feedbacks and Pull requests are welcome. 

For major changes, please reach out both [me](https://www.linkedin.com/in/ivan-nardini/) or 
[Artem Glazkov, SAS Russia](https://www.linkedin.com/in/artem-glazkov-80753824/)