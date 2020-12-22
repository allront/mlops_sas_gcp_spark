import argparse
import logging.config

from src.workflow.jobs.build import run_build
from src.workflow.jobs.migrate import run_migrate

if __name__ == '__main__':

    logging.config.fileConfig("config/logging/local.conf")
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description="Run workflow jobs")
    subparsers = parser.add_subparsers()

    # Build subparser
    sb_build = subparsers.add_parser("build", description="Download Champion Model Content from SAS Model Manager")
    sb_build.add_argument('--project-name', default='Network anomaly detection', help='The name of the model to deploy')
    sb_build.add_argument('--requirements', default='requirement.txt', help='Requirements file')
    sb_build.add_argument('--train-script', default='train.py', help='Script to train the model on GCP')
    sb_build.add_argument('--configfile', default='demo-config.yml', help='configuration yaml file')
    sb_build.add_argument('--score-script', default='score.py', help='Script to score the model on GCP')
    sb_build.set_defaults(func=run_build)

    # Migrate subparser
    sb_migrate = subparsers.add_parser("migrate", description="Load Model content on Google Cloud Platform")
    sb_migrate.add_argument('--bucket-name', default='network-spark-migrate', help='The name of the model to deploy')
    sb_migrate.set_defaults(func=run_migrate)

    args = parser.parse_args()
    args.func(args)
