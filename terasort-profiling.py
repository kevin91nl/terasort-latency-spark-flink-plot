import pickle

from connector.csv import parse_experiment_csv
from dateutil.parser import parse

import matplotlib.pyplot as plt
from connector.ganglia import Ganglia
from plotter.plotter import Plotter
from runner.spark import Spark
from datetime import datetime
import matplotlib.mlab as mlab
import numpy as np
import pandas as pd
from util.format import BytesFormatter
from util.profiling import get_average_profile
from util.time import parse_time_ymdhis
import matplotlib

plt.style.use('ggplot')

df = parse_experiment_csv('data/terasort.csv')
df_spark = df[(df['partitions'] == 10) & (df['size'] == 1000000000) & (df['framework'] == 'Apache Spark') & (
df['experiment'] == 'TeraSort')]
df_flink = df[(df['partitions'] == 10) & (df['size'] == 1000000000) & (df['framework'] == 'Apache Flink') & (
df['experiment'] == 'TeraSort')]
experiments_spark = [[row['start'], row['end']] for index, row in df_spark.iterrows()]
experiments_flink = [[row['start'], row['end']] for index, row in df_flink.iterrows()]

hosts = ['haperf%d.cern.ch' % i for i in range(100, 111)]

import os
if not os.path.exists('terasort-size.pickle'):
    totals_spark = get_average_profile(experiments_spark, hosts)
    totals_flink = get_average_profile(experiments_flink, hosts)
    with open('terasort-size.pickle', 'wb') as file:
        pickle.dump([totals_flink, totals_spark], file)
else:
    with open('terasort-size.pickle', 'rb') as file:
        [totals_flink, totals_spark] = pickle.load(file)
mean_spark = pd.concat(totals_spark).groupby('time').mean()
std_spark = pd.concat(totals_spark).groupby('time').std().fillna(0)
mean_flink = pd.concat(totals_flink).groupby('time').mean()
std_flink = pd.concat(totals_flink).groupby('time').std().fillna(0)


def plot_disk_usage(mean, std, framework):
    fig, ax = plt.subplots()
    ax.plot(mean.index, mean['disk_read'], 'b', label='Read')
    ax.fill_between(mean.index, mean['disk_read'] - std['disk_read'], mean['disk_read'] + std['disk_read'],
                    facecolor='b', alpha=0.5)
    ax.plot(mean.index, mean['disk_write'], 'r', label='Write')
    ax.fill_between(mean.index, mean['disk_write'] - std['disk_write'],
                    mean['disk_write'] + std['disk_write'], facecolor='r', alpha=0.5)

    ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(BytesFormatter(postfix='/s').format))
    ax.set_ylim(bottom=0)
    ax.legend(loc=2)
    lbl_framework = 'Apache Flink' if framework == 'flink' else 'Apache Spark'
    ax.set_title('Disk I/O ' + lbl_framework)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Throughput')

    fig.savefig('output/plots/terasort-disk-' + framework + '.png')


def plot_network_usage(mean, std, framework):
    fig, ax = plt.subplots()
    ax.plot(mean.index, mean['network_read'], 'b', label='Incoming traffic')
    ax.fill_between(mean.index, mean['network_read'] - std['network_read'], mean['network_read'] + std['network_read'],
                    facecolor='b', alpha=0.5)
    ax.plot(mean.index, mean['network_write'], 'r', label='Outgoing traffic')
    ax.fill_between(mean.index, mean['network_write'] - std['network_write'],
                    mean['network_write'] + std['network_write'], facecolor='r', alpha=0.5)

    ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(BytesFormatter(postfix='/s').format))
    ax.set_ylim(bottom=0)
    ax.legend(loc=2)
    lbl_framework = 'Apache Flink' if framework == 'flink' else 'Apache Spark'
    ax.set_title('Network Usage ' + lbl_framework)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Network usage')

    fig.savefig('output/plots/terasort-network-' + framework + '.png')


def plot_combined_network_usage(mean_flink, std_flink, mean_spark, std_spark):
    fig, ax = plt.subplots()
    ax.plot(mean_flink.index, mean_flink['network_read'], 'b', label='Apache Flink')
    ax.fill_between(mean_flink.index, mean_flink['network_read'] - std_flink['network_read'], mean_flink['network_read'] + std_flink['network_read'],
                    facecolor='b', alpha=0.5)

    ax.plot(mean_spark.index, mean_spark['network_read'], 'r', label='Apache Spark')
    ax.fill_between(mean_spark.index, mean_spark['network_read'] - std_spark['network_read'],
                    mean_spark['network_read'] + std_spark['network_read'],
                    facecolor='r', alpha=0.5)

    ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(BytesFormatter(postfix='/s').format))
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.set_title('Network Usage (incoming traffic)')
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Network usage')

    fig.savefig('output/plots/terasort-network-in.png')

    fig, ax = plt.subplots()
    ax.plot(mean_flink.index, mean_flink['network_write'], 'b', label='Apache Flink')
    ax.fill_between(mean_flink.index, mean_flink['network_write'] - std_flink['network_write'],
                    mean_flink['network_write'] + std_flink['network_write'],
                    facecolor='b', alpha=0.5)

    ax.plot(mean_spark.index, mean_spark['network_write'], 'r', label='Apache Spark')
    ax.fill_between(mean_spark.index, mean_spark['network_write'] - std_spark['network_write'],
                    mean_spark['network_write'] + std_spark['network_write'],
                    facecolor='r', alpha=0.5)

    ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(BytesFormatter(postfix='/s').format))
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.set_title('Network Usage (outgoing traffic)')
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Network usage')

    fig.savefig('output/plots/terasort-network-out.png')


def plot_combined_cpu_usage(mean_flink, std_flink, mean_spark, std_spark):
    fig, ax = plt.subplots()
    ax.plot(mean_flink.index, mean_flink['cpu'], 'b', label='Apache Flink')
    ax.fill_between(mean_flink.index, mean_flink['cpu'] - std_flink['cpu'], mean_flink['cpu'] + std_flink['cpu'],
                    facecolor='b', alpha=0.5)

    ax.plot(mean_spark.index, mean_spark['cpu'], 'r', label='Apache Spark')
    ax.fill_between(mean_spark.index, mean_spark['cpu'] - std_spark['cpu'],
                    mean_spark['cpu'] + std_spark['cpu'],
                    facecolor='r', alpha=0.5)

    ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, y: str(int(x)) + "%"))
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.set_title('CPU Usage')
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('CPU usage')

    fig.savefig('output/plots/terasort-cpu.png')


plot_disk_usage(mean_spark, std_spark, 'spark')
plot_disk_usage(mean_flink, std_flink, 'flink')
plot_network_usage(mean_spark, std_spark, 'spark')
plot_network_usage(mean_flink, std_flink, 'flink')
plot_combined_network_usage(mean_flink, std_flink, mean_spark, std_spark)
plot_combined_cpu_usage(mean_flink, std_flink, mean_spark, std_spark)