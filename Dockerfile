FROM apache/airflow:2.6.0-python3.9
USER root
LABEL authors="vuh1"

COPY ./script/airflow/entrypoint-server.sh /opt/airflow/script/entrypoint-server.sh
COPY ./script/airflow/entrypoint-scheduler.sh /opt/airflow/script/entrypoint-scheduler.sh
COPY ./script/airflow/airflow-health-check.sh /opt/airflow/script/airflow-health-check.sh

RUN chmod +x /opt/airflow/script/entrypoint-server.sh  \
    && chmod +x /opt/airflow/script/airflow-health-check.sh  \
    && chmod +x /opt/airflow/script/entrypoint-scheduler.sh

USER airflow

COPY ./requirements.txt /opt/airflow/requirements.txt
RUN pip install --user -r requirements.txt

ENTRYPOINT ["/opt/airflow/script/entrypoint-server.sh"]