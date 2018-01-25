#!/bin/bash

pid=`cat pid.txt`
kill $pid
rm pid.txt
