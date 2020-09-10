"""CLI to run EMR clusters.

Execute command to get help message:
    python cli.py
"""
import random
import string
import time

import boto3
import click
import paramiko


@click.group()
def cli():
    pass


def _wait_until_cluster_ready(emr_client, cluster_id):
    """Wait until cluster is ready."""
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
            ]).format(
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
        if d["Cluster"]["Status"]["State"] == "WAITING":
            return d["Cluster"]["MasterPublicDnsName"]

        time.sleep(10)


def _bootstrap_cluster(host, key_filename):
    def _call_command(ssh_client, command):
        click.echo("Executing command: {0}".format(command))
        _, stdout, stderr = ssh_client.exec_command(command)
        # for line in stdout:
        #     click.echo(line)
        # for line in stderr:
        #     click.echo(line)

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(host, username="hadoop", key_filename=key_filename)

    # Install RStudio.
    _call_command(ssh_client, "sudo yum update")
    _call_command(ssh_client, "sudo yum install libcurl-devel openssl-devel R-devel")
    _call_command(ssh_client, "wget https://download2.rstudio.org/server/centos6/x86_64/rstudio-server-rhel-1.3.1073-x86_64.rpm")
    _call_command(ssh_client, "sudo yum install rstudio-server-rhel-1.3.1073-x86_64.rpm")

    # Add user.
    _call_command(ssh_client, "sudo useradd -m rstudio-user")
    _call_command(ssh_client, "echo 'thepassword123' | sudo passwd --stdin rstudio-user")

    # Create home folder in hdfs.
    _call_command(ssh_client, "hadoop fs -mkdir /user/rstudio-user")
    _call_command(ssh_client, "hadoop fs -chmod 777 /user/rstudio-user")

    # Download R packages.
    _call_command(ssh_client, "aws s3 cp s3://com.credolab.packages/packages-20200326.tar /home/rstudio-user/R/credo-packages.tar")


@cli.command()
@click.option("--name", help="Cluster name")
@click.option("--release", default="emr-6.1.0", help="EMR release label")
@click.option("--ec2-key-pair", default="emr", help="EC2 key pair")
@click.option("--region-name", default="ap-southeast-1", help="EMR region name")
@click.option("--driver-type", default="m5.xlarge", help="Driver instance type")
@click.option("--worker-type", default="m5.xlarge", help="Worker instance type")
@click.option("--worker-count", default=1, help="Worker instance count")
@click.option("--worker-market", default="ON_DEMAND", help="Worker instance market")
@click.option("--key-filename", required=True, help="EC2 key filename")
def create_cluster(
    name, release, ec2_key_pair, region_name,
    driver_type, worker_type, worker_count, worker_market,
    key_filename,
):
    if not name:
        cluster_hash = "".join(random.choice(string.digits) for _ in range(5))
        name = "MY-CLUSTER-{0}".format(cluster_hash)

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
                "InstanceType": driver_type,
                "InstanceCount": 1,
            }, {
                "Name": "Slave nodes",
                "Market": worker_market,
                "InstanceRole": "CORE",
                "InstanceType": worker_type,
                "InstanceCount": worker_count,
            }],
            "Ec2KeyName": ec2_key_pair,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        ServiceRole="EMR_DefaultRole",
        JobFlowRole="EMR_EC2_DefaultRole",
    )["JobFlowId"]
    click.echo("EMR cluster with id={0} submitted to start".format(cluster_id))

    click.echo("")
    click.echo("Waiting until cluster is ready")
    host = _wait_until_cluster_ready(emr_client, cluster_id)

    click.echo("")
    click.echo("Bootstrapping cluster")
    _bootstrap_cluster(host, key_filename)

    click.echo("")
    click.echo("Cluster is ready to use")


if __name__ == "__main__":
    cli()
