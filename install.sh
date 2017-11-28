#!/bin/bash

BASE_DIR=~/tools
DOWN_DIR=~/Downloads
mkdir $BASE_DIR $DOWN_DIR

### Install Virtuoso
cd $DOWN_DIR
V_VERSION=7.2.4.2
V_NAME=virtuoso-opensource-$V_VERSION
V_DIR=$BASE_DIR/virtuoso
V_FILE=$V_NAME.tar.gz
#wget https://github.com/openlink/virtuoso-opensource/releases/download/v$V_VERSION/$V_FILE
#tar -xzvf $V_FILE
#rm $V_FILE
#mv $V_NAME $BASE_DIR
#sudo apt-get install autoconf automake libtool flex bison gperf gawk m4 make openssl libssl-dev
#cd $BASE_DIR/$V_NAME
#CFLAGS="-O2 -m64"
#export CFLAGS
#./configure --prefix=$V_DIR
#make
#make install

### Install Redis
#cd $DOWN_DIR
#wget http://download.redis.io/redis-stable.tar.gz
#tar -xzvf redis-stable.tar.gz
#mv redis-stable $BASE_DIR
#cd $BASE_DIR/redis-stable
#make

# Make dirs
DATA_DIR=~/data
mkdir $DATA_DIR
mkdir $DATA_DIR/virtuoso
mkdir $DATA_DIR/redis
