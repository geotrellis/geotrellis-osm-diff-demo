#!/bin/bash

# Notes:
# Use [m5ad|m5d].xlarge for scratch space

set -e

sudo su

lsblk
mkfs -t ext4 /dev/nvme1n1
mkdir /mnt/data
mount /dev/nvme1n1 /mnt/data

yum install -y tmux

## Install Tippecanoe
yum groupinstall -y "Development Tools"
yum install -y sqlite-devel

git clone https://github.com/mapbox/tippecanoe.git
pushd tippecanoe
make -j
make install
popd
export PATH=/usr/local/bin:"${PATH}"

## Install mbutil
easy_install mbutil
