#!/bin/bash

deactivate
rm -rf .venv
rm -rf .dagster
rm -rf src/etl.egg-info

python3.10 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
python -m pip install -e ".[mysql]"

# export DAGSTER_HOME=$(pwd)

mkdir .dagster
export DAGSTER_HOME=$(pwd)/.dagster
cp dagster.yaml .dagster/dagster.yaml
# dagster-daemon run &
# dagit -w workspace.yaml
