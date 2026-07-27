"""amoCRM → PostgreSQL ETL uchun Airflow DAG.

Har bir table (resource) ALOHIDA Airflow task bo'ladi — shunda Airflow UI'da
har biri bo'yicha status/log/xato/retry alohida ko'rinadi (per-table monitoring).
Task'lar ketma-ket ishlaydi: dlt state xavfsiz bo'lishi va amoCRM rate-limitini
(6 req/s) hurmat qilish uchun (parallel hammering yo'q).

Natijalar `etl_run_log` jadvaliga ham yoziladi (amocrm/runner.py).
"""

from __future__ import annotations

import os                                                # full_refresh env'ini o'rnatish uchun

import pendulum                                          # start_date uchun timezone-aware sana
from airflow import DAG                                  # DAG konteyneri
from airflow.operators.python import PythonOperator      # Python funksiyani task qiladigan operator

from amocrm.runner import run_table                       # bitta table'ni yuklaydigan yadro funksiya
from amocrm.source import RESOURCE_NAMES                  # table (resource) nomlari ro'yxati


def _run(table_name: str, **context) -> int:
    """PythonOperator chaqiradigan o'rovchi: Airflow run_id'ni run_table'ga uzatadi.

    To'liq qayta yuklash: DAG'ni "Trigger DAG w/ config" orqali {"full_refresh": true}
    bilan ishga tushiring — shunda shu run'da barcha jadval noldan yuklanadi
    (konteynerni qayta ishga tushirish shart emas).
    """
    dag_run = context.get("dag_run")                     # shu DAG-run obyekti
    conf = (getattr(dag_run, "conf", None) or {})        # trigger paytida berilgan config
    if conf.get("full_refresh"):                         # {"full_refresh": true} berilgan bo'lsa
        os.environ["AMOCRM_FULL_REFRESH"] = "1"          # runner shu env'ni ko'rib to'liq reload qiladi

    dag_run_id = context.get("run_id", "")               # shu DAG-run identifikatori
    return run_table(table_name, dag_run_id=dag_run_id)  # table'ni yuklaymiz


# DAG'ning umumiy sozlamalari.
default_args = {
    "owner": "amocrm",                                   # egasi
    "retries": 2,                                        # xatoda task 2 marta qayta uriniladi
    "retry_delay": pendulum.duration(minutes=5),         # urinishlar orasidagi tanaffus
}

with DAG(
    dag_id="amocrm_etl",                                 # UI'da ko'rinadigan DAG nomi
    description="amoCRM → PostgreSQL incremental ETL (dlt), har table alohida task",
    schedule="*/15 * * * *",                             # har 15 daqiqada
    start_date=pendulum.datetime(2026, 7, 26, tz="UTC"), # boshlanish sanasi
    catchup=False,                                       # o'tmish uchun run'lar yaratilmaydi
    max_active_runs=1,                                   # run'lar ustma-ust tushmaydi
    default_args=default_args,
    tags=["amocrm", "etl", "dlt"],                       # UI filtrlash uchun teglar
) as dag:

    previous_task = None                                 # ketma-ket zanjir qurish uchun
    for name in RESOURCE_NAMES:                          # har bir table uchun bitta task
        task = PythonOperator(
            task_id=f"load_{name}",                      # UI'da: load_leads, load_contacts, ...
            python_callable=_run,                        # yuqoridagi o'rovchi
            op_kwargs={"table_name": name},              # qaysi table'ni yuklash
        )
        if previous_task is not None:                    # oldingi task bo'lsa
            previous_task >> task                        # ketma-ket bog'laymiz (t1 >> t2)
        previous_task = task                             # keyingi iteratsiya uchun eslab qolamiz
