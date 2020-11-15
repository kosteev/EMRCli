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

# Password to access Jupyter/RStudio.
APP_PASSWORD = "thepassword123"
# from notebook.auth import passwd; passwd(APP_PASSWORD)
JUPYTER_SERVER_PASSWORD = "argon2:$argon2id$v=19$m=10240,t=10,p=8$hp6s0Tp9DBwcz0j85WNsPw$NDDit3Ag+FnT/0EHei9N+g"


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


def _bootstrap_cluster(host, key_filename, r_packages_s3_path, jupyter, rstudio):
    def _call_command(ssh_client, command, wait=True):
        click.echo("")
        click.echo("Executing command: {0}".format(command))
        _, stdout, _ = ssh_client.exec_command(command)
        if not wait:
            return

        stdout.channel.recv_exit_status()
        lines = stdout.readlines()
        if len(lines) > 4:
            # Print first and last 2 lines.
            click.echo(lines[0].strip())
            click.echo(lines[1].strip())
            click.echo("." * 20)
            click.echo(lines[-2].strip())
            click.echo(lines[-1].strip())
        else:
            for l in lines:
                click.echo(l.strip())

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(host, username="hadoop", key_filename=key_filename)

    # --- Jupyter section ---.
    if jupyter:
        # 1. Install Jupyter.
        _call_command(ssh_client, "sudo pip3 install jupyter==1.0.0")
        # 2. Install findspark.
        _call_command(ssh_client, "sudo pip3 install findspark==1.4.2")
        # 3. Run Jupyter server.
        _call_command(
            ssh_client,
            "jupyter notebook --ip 0.0.0.0 --NotebookApp.password='{0}'".format(JUPYTER_SERVER_PASSWORD),
            wait=False,
        )

    # --- RStudio section ---.
    if rstudio:
        # 1. Install RStudio.
        _call_command(ssh_client, "sudo yum -y update")
        _call_command(ssh_client, "sudo yum -y install libcurl-devel openssl-devel R-devel")
        _call_command(ssh_client, "wget https://download2.rstudio.org/server/centos6/x86_64/rstudio-server-rhel-1.3.1073-x86_64.rpm")
        _call_command(ssh_client, "sudo yum -y install rstudio-server-rhel-1.3.1073-x86_64.rpm")
        # 2. Add user.
        _call_command(ssh_client, "sudo useradd -m rstudio-user")
        _call_command(ssh_client, "echo '{0}' | sudo passwd --stdin rstudio-user".format(APP_PASSWORD))
        # 3. Create home folder in hdfs.
        _call_command(ssh_client, "hadoop fs -mkdir /user/rstudio-user")
        _call_command(ssh_client, "hadoop fs -chmod 777 /user/rstudio-user")
        # 4. Download/install R packages.
        if r_packages_s3_path:
            _call_command(ssh_client, "sudo aws s3 cp {0} /home/rstudio-user/packages.tar".format(r_packages_s3_path))
            _call_command(ssh_client, "sudo tar -xvf /home/rstudio-user/packages.tar -C /home/rstudio-user/")

    ssh_client.close()


@cli.command()
@click.option("--name", help="Cluster name")
@click.option("--release", default="emr-5.31.0", help="EMR release label")
@click.option("--ec2-key-pair", default="emr", help="EC2 key pair")
@click.option("--region-name", default="ap-southeast-1", help="EMR region name")
@click.option("--driver-type", default="m5.xlarge", help="Driver instance type")
@click.option("--worker-type", default="m5.xlarge", help="Worker instance type")
@click.option("--worker-count", default=1, help="Worker instance count")
@click.option("--worker-market", default="ON_DEMAND", help="Worker instance market")
@click.option("--key-filename", required=True, help="EC2 key filename")
@click.option("--jupyter/--no-jupyter", default=False, help="Install Jupyter")
@click.option("--rstudio/--no-rstudio", default=False, help="Install RStudio")
@click.option("--r-packages-s3-path", help="R packages to install")
def create_cluster(
    name, release, ec2_key_pair, region_name,
    driver_type, worker_type, worker_count, worker_market,
    key_filename, jupyter, rstudio, r_packages_s3_path,
):
    """Create EMR cluster.

    Once cluster is up, visit links from the output.
    """
    if (not jupyter and
            not rstudio):
        raise ValueError("Either `--jupyter` or `--rstudio` should be specified")

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
        Configurations=[{
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true",
            },
        }, {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.dynamicAllocation.enabled": "false",
            },
        }],
        ServiceRole="EMR_DefaultRole",
        JobFlowRole="EMR_EC2_DefaultRole",
    )["JobFlowId"]
    click.echo("EMR cluster with id={0} submitted to start".format(cluster_id))

    click.echo("")
    click.echo("Waiting until cluster is ready")
    host = _wait_until_cluster_ready(emr_client, cluster_id)

    click.echo("")
    click.echo("Bootstrapping cluster")
    _bootstrap_cluster(
        host, key_filename, r_packages_s3_path,
        jupyter=jupyter, rstudio=rstudio,
    )

    click.echo("")
    # ec2-3-0-101-87.ap-southeast-1.compute.amazonaws.com -> 3.0.101.87
    ip = host[4:].split(".")[0].replace("-", ".")
    click.echo("-" * 60)
    click.echo("Cluster is ready to use.")
    if jupyter:
        click.echo("Jupyter link: http://{0}:8888. Password: '{1}'.".format(ip, APP_PASSWORD))
    if rstudio:
        click.echo("RStudio link: http://{0}:8787. User: 'rstudio-user', password: '{1}'.".format(ip, APP_PASSWORD))
    click.echo("Spark History Server: http://{0}:18080/?showIncomplete=true.".format(ip))


if __name__ == "__main__":
    cli()
