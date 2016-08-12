from util.time import parse_time_ymdhis
import pandas as pd


def parse_experiment_csv(file):
    """
    Read the experiment data CSV into a usable structure.

    :param file: Path to the CSV file
    :return: List of records
    """
    with open(file, 'r') as file_handle:
        lines = file_handle.readlines()
    data = []
    for line in lines[1:]:
        if len(line.strip()) > 0:
            line = line.replace('"', '')
            new_experiment, new_framework, start_date, start_time, end_date, end_time, new_arguments = line.strip().split(
                ',')
            experiment = new_experiment if len(new_experiment) > 0 else experiment
            framework = new_framework if len(new_framework) > 0 else framework
            arguments = new_arguments if len(new_arguments) > 0 else arguments
            start = parse_time_ymdhis(start_date + start_time)
            end = parse_time_ymdhis(end_date + end_time)
            partitions = int(arguments.split('=')[1].split('/')[0])
            size = int(arguments.split('=')[-1])
            data += [[experiment, framework, start, end, partitions, size]]

        df = pd.DataFrame(data, columns=['experiment', 'framework', 'start', 'end', 'partitions', 'size'])

    # Calculate difference (in seconds)
    df['diff'] = (df['end'] - df['start']).apply(lambda x: x.total_seconds())

    return df
