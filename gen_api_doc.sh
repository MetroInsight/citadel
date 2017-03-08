#!/bin/bash

cd doc/api
java -jar ./swagger-codegen-cli-2.2.1.jar generate -l html -i swagger_schema.json
