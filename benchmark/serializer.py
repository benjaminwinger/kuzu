import os
import logging
import shutil
import sys
import subprocess
import re
import argparse

base_dir = os.path.dirname(os.path.realpath(__file__))
kuzu_exec_path = os.path.join(
    base_dir, '..', 'build', 'release', 'tools', 'shell', 'kuzu_shell')


def _get_kuzu_version():
    cmake_file = os.path.join(base_dir, '..', 'CMakeLists.txt')
    with open(cmake_file) as f:
        for line in f:
            if line.startswith('project(Kuzu VERSION'):
                return line.split(' ')[2].strip()


def serialize(dataset_name, dataset_path, serialized_graph_path, benchmark_copy_log_dir):
    bin_version = _get_kuzu_version()

    if not os.path.exists(serialized_graph_path):
        os.mkdir(serialized_graph_path)

    if os.path.exists(os.path.join(serialized_graph_path, 'version.txt')):
        with open(os.path.join(serialized_graph_path, 'version.txt')) as f:
            dataset_version = f.readline().strip()
            if dataset_version == bin_version:
                logging.info(
                    'Dataset %s has version of %s, which matches the database version, skip serializing', dataset_name,
                    bin_version)
                return
            else:
                logging.info(
                    'Dataset %s has version of %s, which does not match the database version %s, serializing dataset...',
                    dataset_name, dataset_version, bin_version)
    else:
        logging.info('Dataset %s does not exist or does not have a version file, serializing dataset...', dataset_name)

    shutil.rmtree(serialized_graph_path, ignore_errors=True)
    os.mkdir(serialized_graph_path)

    serialize_queries = []
    if os.path.exists(os.path.join(dataset_path, 'schema.cypher')):
        with open(os.path.join(dataset_path, 'schema.cypher'), 'r') as f:
            serialize_queries += f.readlines()
        with open(os.path.join(dataset_path, 'copy.cypher'), 'r') as f:
            serialize_queries += f.readlines()
    else:
        with open(os.path.join(base_dir, 'serialize.cypher'), 'r') as f:
            serialize_queries += f.readlines()
    serialize_queries = [q.strip().replace('{}', dataset_path)
                         for q in serialize_queries]

    table_types = {}

    for s in serialize_queries:
        logging.info('Executing query: %s', s)
        create_match = re.match('create\s+(.+?)\s+table\s+(.+?)\s*\(', s, re.IGNORECASE)
        # Run kuzu shell one query at a time. This ensures a new process is
        # created for each query to avoid memory leaks.
        process = subprocess.Popen([kuzu_exec_path, serialized_graph_path],
            stdin=subprocess.PIPE, stdout=sys.stdout if create_match else subprocess.PIPE, encoding="utf-8")
        output, _ = process.communicate(s + ";\n")
        if create_match:
            table_types[create_match.group(2)] = create_match.group(1).lower()
        else:
            copy_match = re.match('copy\s+(.+?)\s+from', s, re.IGNORECASE)
            filename = table_types[copy_match.group(1)] + '-' + copy_match.group(1).replace('_', '-') + '_log.txt'
            os.makedirs(benchmark_copy_log_dir, exist_ok=True)
            with open(os.path.join(benchmark_copy_log_dir, filename), 'a', encoding="utf-8") as f:
                for line in output.splitlines():
                    print(line)
                    print(line, file=f)
        if process.returncode != 0:
            raise RuntimeError(f'Error {process.returncode} executing query: {s}')

    with open(os.path.join(serialized_graph_path, 'version.txt'), 'w') as f:
        f.write(bin_version)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Serializes dataset to a kuzu database')
    parser.add_argument("dataset_name", help="Name of the dataset for dispay purposes")
    parser.add_argument("dataset_path", help="Input path of the dataset to serialize")
    parser.add_argument("serialized_graph_path", help="Output path of the database. Will be created if it does not exist already")
    parser.add_argument("benchmark_copy_log_dir")
    args = parser.parse_args()
    try:
        serialize(args.dataset_name, args.dataset_path, args.serialized_graph_path, args.benchmark_copy_log_dir)
    except Exception as e:
        logging.error(f'Error serializing dataset {args.dataset_name}')
        raise e
    finally:
        shutil.rmtree(os.path.join(base_dir, 'history.txt'),
                      ignore_errors=True)
