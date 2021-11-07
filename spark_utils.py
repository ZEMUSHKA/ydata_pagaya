#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess
import json
import re
import math


def get_free_mem_mb():
    with open('/proc/meminfo') as file:
        for line in file:
            if 'MemAvailable' in line:
                return int(line.split()[1]) // 1024
    return 1024  # safe default


def copy_ssh_key_hint():
    return "scp -i ~/andrey.pem ~/andrey.pem hadoop@{}:~".format(get_dns_name())


def get_dns_name():
    return json.loads(subprocess.check_output(
        "aws ec2 describe-instances --instance-ids $(ec2-metadata -i | cut -d' ' -f2) --query 'Reservations[].Instances[].PublicDnsName'",
        shell=True
    ))[0]


def get_app_id():
    try:
        return re.search(".*(application[_0-9]+).*", str(subprocess.check_output('yarn application -list', shell=True))).groups()[0]
    except:
        return None


def print_ui_links():
    dns_name = get_dns_name()
    app_id = get_app_id()
    print("NameNode: http://{}:50070".format(dns_name))
    print("YARN: http://{}:8088".format(dns_name))
    if app_id is not None:
        print("Spark UI: http://{}:20888/proxy/{}".format(dns_name, app_id))


def get_spark_conf(total_memory_per_core=7800, cores_per_executor=4,
                   parallelism=500, add_python_memory=True,
                   additional_conf={}):
    """
    spark.executor.memory: jvm heap
    spark.yarn.executor.memoryOverhead: jvm offheap + python
    total per yarn container: jvm heap + jvm offheap + python
    """
    import pyspark
    offheap_memory_per_core = max(1024, int(total_memory_per_core * 0.07))
    python_memory_per_core = 0
    if add_python_memory:
        # python eats ~ the same amount as jvm heap
        python_memory_per_core = (total_memory_per_core - offheap_memory_per_core) // 2
    heap_memory_per_core = total_memory_per_core - offheap_memory_per_core - python_memory_per_core
    free_mem_mb = get_free_mem_mb()
    conf = (
        pyspark.SparkConf()
        .set("spark.executor.memory", "{0}m".format(heap_memory_per_core * cores_per_executor))
        .set("spark.yarn.executor.memoryOverheadFactor", "0")
        .set("spark.yarn.executor.memoryOverhead", (offheap_memory_per_core + python_memory_per_core) * cores_per_executor)
        .set("spark.python.worker.memory", "{0}m".format(int(python_memory_per_core * 0.8)))
        .set("spark.executor.cores", cores_per_executor)
        .set("spark.default.parallelism", parallelism)
        .set("spark.submit.deployMode", "client")
        .set("spark.driver.memory", "{}m".format(math.ceil(free_mem_mb * 0.5)))
        .set("spark.driver.maxResultSize", "{}m".format(math.ceil(free_mem_mb * 0.4)))
    )
    for k, v in additional_conf.items():
        conf = conf.set(k, v)
    return conf
