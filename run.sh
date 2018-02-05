#!/bin/bash

if [ -f "pid.txt" ]; then
  echo "pid.txt already exists."
else
  nohup java -jar target/Citadel-0.0.1-SNAPSHOT-fat.jar -conf ./src/main/resources/conf/citadel-conf.json &
  echo $! >> pid.txt
fi
