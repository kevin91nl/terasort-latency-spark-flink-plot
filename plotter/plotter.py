from util.format import BytesFormatter
import matplotlib
import matplotlib.pyplot as plt
import numpy as np


class Plotter:
    def __init__(self, api):
        self.api = api

    def plot_network_activity(self, hosts, start=None, end=None):
        bytes_in, bytes_out = None, None
        for host in hosts:
            if bytes_in is None:
                bytes_in = self.api.retrieve_data(metric='bytes_in', start=start, end=end, host=host)
                bytes_out = self.api.retrieve_data(metric='bytes_out', start=start, end=end, host=host)
            else:
                bytes_in[:, 1] += self.api.retrieve_data(metric='bytes_in', start=start, end=end, host=host)[:, 1]
                bytes_out[:, 1] += self.api.retrieve_data(metric='bytes_out', start=start, end=end, host=host)[:, 1]
        fig, ax = plt.subplots(1, 1)
        ax.plot(bytes_in[:, 0], bytes_in[:, 1], 'b', label='Received')
        ax.fill_between(bytes_in[:, 0].T.tolist()[0], np.full(bytes_in.shape[0], bytes_in[:, 1].min()),
                        bytes_in[:, 1].T.tolist()[0], alpha=0.5, facecolor='b')
        ax.plot(bytes_out[:, 0], bytes_out[:, 1], 'r', label='Sent')
        ax.fill_between(bytes_out[:, 0].T.tolist()[0], np.full(bytes_out.shape[0], bytes_in[:, 1].min()),
                        bytes_out[:, 1].T.tolist()[0], alpha=0.5, facecolor='r')
        ax.legend()
        ax.set_title('Network activity')
        ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(BytesFormatter(postfix='/s').format))
        ax.set_ylabel('Throughput')
        ax.set_xlabel('Time (seconds)')
        return fig, ax
