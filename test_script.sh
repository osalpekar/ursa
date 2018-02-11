#!/bin/bash

# create a conda environment for python build, if necessary
uuid=$(uuidgen)
echo $(uuidgen)
conda create -y -q -n ursa-python3-${uuid} python=3
source activate ursa-python3-${uuid}

make prepare

# run flake8
python3 -m flake8 --exclude env

if [ $? -ne 0 ]; then
	echo "Please fix flake8 errors."
	exit -1
fi

# run pytest for python3 on all test files
python3 -m pytest -v test/local_manager_test.py
python3 -m pytest -v test/graph_test.py
python3 -m pytest -v test/serialization_test.py
#python3 -m pytest -v test/test_connected_components.py

# deactivate and remove the conda env
source deactivate
conda remove -y -n ursa-python3-${uuid} --all

# create a conda environment for python build, if necessary
uuid=$(uuidgen)
echo $(uuidgen)
conda create -y -q -n ursa-python2-${uuid} python=2.7
source activate ursa-python2-${uuid}

make prepare

#run pytest for python2 on all test files
python2 -m pytest -v test/local_manager_test.py
python2 -m pytest -v test/graph_test.py
python2 -m pytest -v test/serialization_test.py
#python2 -m pytest -v test/test_connected_components.py

# deactivate and remove the conda env
source deactivate
conda remove -y -n ursa-python2-${uuid} --all
