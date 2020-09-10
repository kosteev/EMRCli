"""CLI to run EMR clusters.

Execute command to get help message:
    python cli.py
"""
import random
import string
import time

import boto3
import click


@click.group()
def cli():
    pass


@cli.command()
@click.option("--name", help="Cluster name")
@click.option("--release", help="EMR release label")
@click.option("--ec2-key-pair", help="EC2 key pair")
@click.option("--region-name", help="EMR region name")
def create_cluster(name, release, ec2_key_pair, region_name):
    # Defaults.
    if not name:
        cluster_hash = "".join(random.choice(string.digits) for _ in range(5))
        name = "MY-CLUSTER-{0}".format(cluster_hash)
    release = release or "emr-6.1.0"
    ec2_key_pair = ec2_key_pair or "20200909_aws_key_pair"
    region_name = region_name or "ap-southeast-1"

    emr_client = boto3.client("emr", region_name=region_name)
    cluster_id = emr_client.run_job_flow(
        Name=name,
        ReleaseLabel=release,
        Applications=[{"Name": "Spark"}],
        Instances={
            "InstanceGroups": [{
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",  # TODO
                "InstanceCount": 1,  # TODO
            }, {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",  # TODO
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",  # TODO
                "InstanceCount": 1,  # TODO
            }],
            "Ec2KeyName": ec2_key_pair,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        ServiceRole="EMR_DefaultRole",
        JobFlowRole="EMR_EC2_DefaultRole",
    )["JobFlowId"]
    click.echo("EMR cluster with id={0} submitted to start".format(cluster_id))

    # Waiting until cluster is ready.
    first = True
    while True:
        d = emr_client.describe_cluster(ClusterId=cluster_id)

        if first:
            first = False
            click.echo("\n".join([
                "Name: {name}",
                "Release: {release}",
                "Applications: {applications}",
                "ARN: {arn}",
                "",
            ]).format(
                id=d["Cluster"]["Id"],
                name=d["Cluster"]["Name"],
                release=d["Cluster"]["ReleaseLabel"],
                applications=", ".join([
                    "{0}={1}".format(app["Name"], app["Version"])
                    for app in d["Cluster"]["Applications"]
                ]),
                arn=d["Cluster"]["ClusterArn"],
            ))
        else:
            click.echo("\n".join([
                "",
                "State: {state}, {state_message}",
                "Master Public DNS: {master_public_dns}",
                "." * 20,
            ]).format(
                state=d["Cluster"]["Status"]["State"],
                state_message=d["Cluster"]["Status"]["StateChangeReason"].get("Message"),
                master_public_dns=d["Cluster"].get("MasterPublicDnsName"),
            ))

        time.sleep(10)


if __name__ == "__main__":
    cli()
