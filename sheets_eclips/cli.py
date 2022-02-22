import base64
import copy
import json
import socket
from datetime import datetime

import click
import requests
from bson.objectid import ObjectId
from click_shell import shell
from loguru import logger
from pyfiglet import Figlet


class DagRun(object):
    DEV_BASE_URL = "http://localhost:8080/api/v1"
    STAGING_BASE_URL = "https://airflow.vr-int.cloud/api/v1"

    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.session = requests.Session()
        authorization_header_token = base64.b64encode(f"{username}:{password}".encode()).decode()  # noqa: E501
        self.session.headers["Authorization"] = f"Basic {authorization_header_token}"  # noqa: E501

    def run(self, dag, conf=None):
        conf = conf if conf else {}
        data = {
            "conf": copy.copy(conf)
        }

        current_date = datetime.utcnow().isoformat(sep='-', timespec='milliseconds')  # noqa: E501
        data["dag_run_id"] = str(ObjectId())

        response = self.session.post(url=f"{self.base_url}/dags/{dag}/dagRuns",
                                     headers={
                                         "Content-Type": "application/json"
                                     },
                                     data=json.dumps(data))
        response.raise_for_status()
        logger.info(response.text)
        return data["dag_run_id"]


@shell(prompt='ventus> ', intro="Running external DataPipelines flows CLI")
@click.pass_context
def cli(ctx):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    # environment = input("Environment (1: staging, 2: dev)")
    environment = 1
    if int(environment) == 1:
        ctx.obj['BASE_URL'] = DagRun.STAGING_BASE_URL
        print(Figlet(font='slant').renderText('Eclipse-Staging'))
    else:
        ctx.obj['BASE_URL'] = DagRun.DEV_BASE_URL
        print(Figlet(font='slant').renderText('Eclipse-Dev'))
    # ctx.obj['USER'] = input("Please enter AirFlow username: ")
    # ctx.obj['PASS'] = getpass.getpass(prompt="Please enter AirFlow password: ")
    ctx.obj['USER'] = 'admin'
    ctx.obj['PASS'] = 'admin'

# @cli.command()
# @click.option("--root_domain", "--root_domain", "-d", required=False)
# @click.option("--name", "--name", "-n", required=False)
# @click.option("--bvd_id", "--bvd_id", "-b", required=False)
# @click.option("--custom_company_filter", "-f", required=False)
# @click.pass_context
# def eclipse(root_domain=None, name=None, bvd_id=None,

def eclipse(root_domain=None, name=None, bvd_id=None,
            custom_company_filter=None):
    base_url = DagRun.STAGING_BASE_URL
    username = 'admin'
    password = 'admin'
    # dag_run = DagRun(base_url=ctx.obj["BASE_URL"], username=ctx.obj['USER'], password=ctx.obj['PASS'])  # noqa: E501
    dag_run = DagRun(base_url=base_url, username=username, password=password)  # noqa: E501


    if not root_domain and not custom_company_filter:
        raise Exception("You must provide company domain or custom filter")

    # build company filter
    if custom_company_filter:
        company_filter = custom_company_filter
    else:
        company_filter = {"root_domain": root_domain}
        if name:
            company_filter["name"] = name
        if bvd_id:
            company_filter["identifiers.bvd"] = bvd_id

    conf = {
        "user": socket.gethostname(),
        "company": company_filter
    }

    return dag_run.run(dag="Eclipse", conf=conf)  # I have altered the run func to return the dag_id

# @cli.command()
# @click.option("--root_domain", "--root_domain", "-d", required=False)
# @click.option("--name", "--name", "-n", required=False)
# @click.option("--bvd_id", "--bvd_id", "-b", required=False)
# @click.option("--custom_company_filter", "-f", required=False)
# @click.pass_context
def horizon(ctx, root_domain=None, name=None, bvd_id=None,
            custom_company_filter=None):
    dag_run = DagRun(base_url=ctx.obj["BASE_URL"], username=ctx.obj['USER'], password=ctx.obj['PASS'])  # noqa: E501

    if not root_domain and not custom_company_filter:
        raise Exception("You must provide company domain or custom filter")

    # build company filter
    if custom_company_filter:
        company_filter = custom_company_filter
    else:
        company_filter = {"root_domain": root_domain}
        if name:
            company_filter["name"] = name
        if bvd_id:
            company_filter["identifiers.bvd"] = bvd_id

    conf = {
        "user": socket.gethostname(),
        "company": company_filter
    }

    dag_run.run(dag="Horizon", conf=conf)


if __name__ == '__main__':
    cli()
