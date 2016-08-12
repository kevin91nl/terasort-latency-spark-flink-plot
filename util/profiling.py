import pandas as pd
from connector.ganglia import Ganglia
from util.time import parse_time_ymdhis


def get_profile_data(start, end, host):
    api = Ganglia("http://haperf100.cern.ch/ganglia", "Cluster", host=host)
    read = None
    write = None
    read1 = api.retrieve_data("diskstat_vda_read_bytes_per_sec", start, end)
    if len(read1) > 1:
        read = read1
    read2 = api.retrieve_data("diskstat_vdb_read_bytes_per_sec", start, end)
    if len(read2) > 1:
        if read is None:
            read = read2
        else:
            read[:, 1] += read2[:, 1]
    write1 = api.retrieve_data("diskstat_vda_write_bytes_per_sec", start, end)
    if len(write1) > 1:
        write = write1
    write2 = api.retrieve_data("diskstat_vdb_write_bytes_per_sec", start, end)
    if len(write2) > 1:
        if write is None:
            write = write2
        else:
            write[:, 1] += write2[:, 1]
    disk_read = pd.DataFrame(read, columns=['time', 'disk_read'])
    disk_write = pd.DataFrame(write, columns=['time', 'disk_write'])
    bytes_in = pd.DataFrame(api.retrieve_data("bytes_in", start, end),
                            columns=['time', 'network_read'])
    bytes_out = pd.DataFrame(api.retrieve_data("bytes_out", start, end),
                             columns=['time', 'network_write'])
    cpu = pd.DataFrame(api.retrieve_data("cpu_system", start, end),
                       columns=['time', 'cpu'])

    df = disk_read \
        .merge(disk_write, on=['time']) \
        .merge(bytes_in, on=['time']) \
        .merge(bytes_out, on=['time']) \
        .merge(cpu, on=['time'])

    df = df.rolling(window=10, center=False).mean()

    return df


def get_totals(start, end, hosts):
    totals = None
    for host in hosts:
        result = get_profile_data(start, end, host)
        if totals is None:
            totals = result
        else:
            totals['disk_read'] += result['disk_read']
            totals['disk_write'] += result['disk_write']
            totals['network_read'] += result['network_read']
            totals['network_write'] += result['network_write']
            totals['cpu'] += result['cpu']
    totals['cpu'] /= len(hosts)
    return totals


def combine_experiments(df1, df2):
    df3 = df1.merge(df2, on=['time'], how='outer').fillna(0)
    df3['disk_write'] = df3['disk_write_x'] + df3['disk_write_y']
    df3['disk_read'] = df3['disk_read_x'] + df3['disk_read_y']
    df3['network_read'] = df3['network_read_x'] + df3['network_read_y']
    df3['network_write'] = df3['network_write_x'] + df3['network_write_y']
    df3['cpu'] = df3['cpu_x'] + df3['cpu_y']
    del df3['disk_write_x']
    del df3['disk_write_y']
    del df3['disk_read_x']
    del df3['disk_read_y']
    del df3['network_read_x']
    del df3['network_read_y']
    del df3['network_write_x']
    del df3['network_write_y']
    del df3['cpu_x']
    del df3['cpu_y']
    return df3


def get_average_profile(experiments, hosts):
    dfs = []
    for start_date, end_date in experiments:
        dfs.append(get_totals(start_date, end_date, hosts))
    return dfs
