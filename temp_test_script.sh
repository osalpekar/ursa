#!/bin/bash

# create a conda environment for python build, if necessary
uuid=$(uuidgen)
echo $(uuidgen)
conda create -y -q -n ursa-python3-${uuid} python=3
source activate ursa-python3-${uuid}

make prepare

python3 -m pytest -v test/graph_test.py
source deactivate
conda remove -y -n ursa-python3-${uuid} --all
