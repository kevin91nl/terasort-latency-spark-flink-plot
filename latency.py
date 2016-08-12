from dateutil.parser import parse

import matplotlib.pyplot as plt
from connector.ganglia import Ganglia
from plotter.plotter import Plotter
from runner.spark import Spark
from datetime import datetime
import matplotlib.mlab as mlab
import numpy as np

plt.style.use('ggplot')


# api = Ganglia("http://haperf100.cern.ch/ganglia", "Cluster")
# plotter = Plotter(api)
# fig, ax = plotter.plot_network_activity(['haperf%d.cern.ch' % i for i in range(100, 111)],
#                                         start=parse('2016-08-08 14:00'), end=parse('2016-08-08 14:30'))
# fig.savefig('test.png')


def microseconds_discretizer(d):
    return d.microsecond + 1000000 * (d.second + 60 * (d.minute + 60 * (d.hour + 24 * (d.day))))


def filter_large(items):
    return [item for item in items if item < 10000]


def get_flink_latency(size):
    with open('data/latency/flink/%d.txt' % size) as file:
        data = file.read()

    return filter_large([int(line.split(" ")[1]) for line in data.split("\n") if line.startswith("1>")])


def get_spark_latency(size):
    with open('data/latency/spark/%d.txt' % size) as file:
        lines = file.readlines()
    delays = []
    for line in lines:
        delay = -1
        try:
            delay = int(line)
            delays.append(delay)
        except ValueError:
            continue
    return filter_large(delays)


size = 2

fig, ax = plt.subplots()

diff_flink = get_flink_latency(size)
n, bins, patches = ax.hist(diff_flink, 100, facecolor='b', alpha=0.5, normed=1, label='Apache Flink')
bin_width = bins[1] - bins[0]
x = np.linspace(min(bins), max(bins), 500)
# y = mlab.normpdf(x, np.mean(diff_flink), np.std(diff_flink))
# ax.plot(x, y, 'b', label='Apache Flink')

diff_spark = get_spark_latency(size)
n, bins, patches = ax.hist(diff_spark, 100, facecolor='r', alpha=0.5, normed=1, label='Apache Spark')
bin_width = bins[1] - bins[0]
x = np.linspace(min(bins), max(bins), 500)
#y = mlab.normpdf(x, np.mean(diff_spark), np.std(diff_spark))
#ax.plot(x, y, 'r', label='Apache Spark')

ax.set_title('Latency Comparison')
ax.set_xlabel('Latency (ms)')
ax.set_ylabel('Frequency')
ax.set_xlim([0, max(np.mean(diff_flink) + 2 * np.std(diff_flink), np.mean(diff_spark) + 2 * np.std(diff_spark))])
ax.legend()
fig.savefig('output/plots/latency.png')

print("mu_flink\t= %.4f" % np.mean(diff_flink))
print("sigma_flink\t= %.4f" % np.std(diff_flink))
print("mu_spark\t= %.4f" % np.mean(diff_spark))
print("sigma_spark\t= %.4f" % np.std(diff_spark))
