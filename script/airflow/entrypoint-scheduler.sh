#!/bin/bash
set -e

$(command -v airflow) db upgrade

exec airflow scheduler
