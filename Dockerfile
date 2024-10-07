FROM apache/airflow:2.2.3
COPY requirements.txt .
COPY ICWSM19_data/ .
RUN pip install -r requirements.txt