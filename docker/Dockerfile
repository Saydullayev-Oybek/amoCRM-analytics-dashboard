# amoCRM ETL uchun Airflow image: rasmiy Airflow ustiga dlt + requests qo'shamiz.
FROM apache/airflow:2.10.5-python3.12

# Qo'shimcha Python paketlarini o'rnatamiz (airflow foydalanuvchisi ostida).
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
