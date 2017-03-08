# FlaskWebService
A simple flask web service to store and retrieve data

## Dependency for native running
Python 3.5+
.
.

## Dependency for Docker
Docker

## Install
Install software listed in requirements.txt and pip-packages.list

Create a database in InfluxDB called 'citadel'
* influx
* CREATE DATABASE citadel

## Running Instruction

### Run natively
python app/main.py

### Running with Docker
1. Install Docker from [here](https://docs.docker.com/engine/installation/linux/ubuntu/)
2. Run "run_with_docker" file. It may ask authority elevation (sudo.)  
   Currently the script is based on Linux environment.  
   Same script can be easily generated for Windows and Mac.


## API Documentation
1. Specify your machine's IP in app/config.py for both CITADEL_HOST and SERVER_NAME
2. Run Citadel with "python app/main.py"
3. API doc will be accessible from your browser at "http://host:post/api/doc"
