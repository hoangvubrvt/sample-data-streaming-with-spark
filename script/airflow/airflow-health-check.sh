#!/bin/bash

# Check if Airflow web server is accessible
response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health)

if [ $response -eq 200 ]; then
  exit 0 # Health check passed
else
  exit 1 # Health check failed
fi