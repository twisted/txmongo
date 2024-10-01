#!/usr/bin/env python3
import argparse
import os
from subprocess import run

parser = argparse.ArgumentParser(description='Run tests with dockerized MongoDB')
parser.add_argument('--mongodb-version', type=str, help='MongoDB version', required=True)
# for test_basic
parser.add_argument('--mongodb-port', type=str, help='MongoDB Port (default 27017)', default='27017')
# for test_auth
parser.add_argument('--mongodb-port-auth', type=str, help='MongoDB Port (default 27018)', default='27018')
# for test_replicaset
parser.add_argument('--mongodb-port-1', type=str, help='MongoDB 1 Replica Port (default 37017)', default='37017')
parser.add_argument('--mongodb-port-2', type=str, help='MongoDB 2 Replica Port (default 37018)', default='37018')
parser.add_argument('--mongodb-port-3', type=str, help='MongoDB 3 Replica Port (default 37019)', default='37019')

args, tox_args = parser.parse_known_args()

mongodb_network_name = 'txmongo-tests-advanced-network'
run(['docker', 'network', 'create', mongodb_network_name])

mongodb_basic_test_container_name = 'txmongo-tests-basic-mongodb'

run(['docker', 'run', '--rm', '-d', '-p', f'{args.mongodb_port}:27017', '--name', mongodb_basic_test_container_name, f'mongo:{args.mongodb_version}'])

run(['tox', *tox_args], env={
    **os.environ,
    'TXMONGO_RUN_MONGOD_IN_DOCKER': 'yes',
    'TXMONGO_MONGOD_DOCKER_VERSION': args.mongodb_version,
    'TXMONGO_MONGOD_DOCKER_PORT_AUTH': args.mongodb_port_auth,
    'TXMONGO_MONGOD_DOCKER_PORT_1': args.mongodb_port_1,
    'TXMONGO_MONGOD_DOCKER_PORT_2': args.mongodb_port_2,
    'TXMONGO_MONGOD_DOCKER_PORT_3': args.mongodb_port_3,
    'TXMONGO_MONGOD_DOCKER_NETWORK_NAME': mongodb_network_name,
})
run(['docker', 'stop', mongodb_basic_test_container_name])

run(['docker', 'network', 'rm', "--force", mongodb_network_name])

# if need manually delete containers:
#           docker container rm --force $(docker ps -q --filter "name=txmongo-tests-")
# run some version:
#           ./bin/run_tests_with_docker.py --mongodb-version 4.4 -f advanced py3.11 pymongo480 tw247