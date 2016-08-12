from dateutil.parser import parse

import matplotlib.pyplot as plt
from connector.ganglia import Ganglia
from plotter.plotter import Plotter
from runner.spark import Spark

plt.style.use('ggplot')

# api = Ganglia("http://haperf100.cern.ch/ganglia", "Cluster")
# plotter = Plotter(api)
# fig, ax = plotter.plot_network_activity(['haperf%d.cern.ch' % i for i in range(100, 111)],
#                                         start=parse('2016-08-08 14:00'), end=parse('2016-08-08 14:30'))
# fig.savefig('test.png')

Spark()