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

plt.style.use('ggplot')

df = parse_experiment_csv('data/terasort.csv')

# Extract for both platforms
terasort_spark = df[(df['framework'] == 'Apache Spark') & (df['partitions'] == 10)]
terasort_flink = df[(df['framework'] == 'Apache Flink') & (df['partitions'] == 10)]

# Create the plots
fig, ax = plt.subplots()
ind = np.arange(4)
width = 0.35

# Create the bars
bars_flink = ax.bar(ind, terasort_flink.groupby('size').mean()['diff'].tolist(), width, color='b', yerr=terasort_flink.groupby('size').std()['diff'].tolist(), alpha=0.5, error_kw=dict(ecolor='b'))
bars_spark = ax.bar(ind + width, terasort_spark.groupby('size').mean()['diff'].tolist(), width, color='r', yerr=terasort_spark.groupby('size').std()['diff'].tolist(), alpha=0.5, error_kw=dict(ecolor='r'))

# Create some labels
ax.set_xlabel('Dataset size')
ax.set_ylabel('Execution time (s)')
ax.set_title('TeraSort Duration')
ax.set_xticks(ind + width)
ax.set_xticklabels(('100MB', '1GB', '10GB', '100GB'))
ax.legend(['Apache Flink', 'Apache Spark'], loc=0)

# Save the plot
fig.savefig('output/plots/terasort-duration.png')