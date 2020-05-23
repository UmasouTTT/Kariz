#!/usr/bin/python3

import workload.sequential as sqw
import sys

config_file = sys.argv[1]

sqw.Sequential(config_file)
