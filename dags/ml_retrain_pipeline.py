import os
import random
import requests
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

logger = logging.getLogger(__name__)


MODEL_VERSION = os.getenv("MODEL_VERSION")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ACCURACY_THRESHOLD = float(os.getenv("ACCURACY_THRESHOLD"))
F1_THRESHOLD = float(os.getenv("F1_THRESHOLD"))


def load_data(**context):
    """Загрузка данных из какого то источника"""
    logger.info("Загрузка данных для обучения модели")


def train_model(**context):
    """Обучение модели"""
    logger.info(f"Обучение модели {MODEL_VERSION}")


def evaluate_model(**context):
    """Оценка модели"""
    logger.info("Оценка модели...")
    random_accuracy = random.uniform(0.75, 0.8)
    metrics = {
        "accuracy": round(random_accuracy, 4),
        "f1_score": round(random_accuracy - 0.02, 4) # поменьше чем accuracy
    }
    logger.info(f"Метрики: accuracy={metrics['accuracy']}, f1={metrics['f1_score']}")
    context["ti"].xcom_push(key="metrics", value=metrics)


def check_quality(**context):
    """Проверка качества модели"""
    metrics = context["ti"].xcom_pull(key="metrics", task_ids="evaluate_model")
    acc = metrics["accuracy"]
    f1 = metrics["f1_score"]

    logger.info(f"Пороги: accuracy ≥ {ACCURACY_THRESHOLD}, f1 ≥ {F1_THRESHOLD}")
    logger.info(f"Текущие значения: accuracy={acc}, f1={f1}")

    if acc >= ACCURACY_THRESHOLD and f1 >= F1_THRESHOLD:
        logger.info("Проверка качества пройдена — запуск деплоя")
        return "deploy_model"
    else:
        logger.info("Проверка качества не пройдена — деплой отменён")
        return "skip_deploy"


def deploy_model(**context):
    """Деплой модели"""
    logger.info(f"Модель {MODEL_VERSION} развернута в продакшене")
    deployment_info = {
        "deploy_time": context["ds"],
        "metrics": context["ti"].xcom_pull(key="metrics", task_ids="evaluate_model"),
    }
    context["ti"].xcom_push(key="deployment_info", value=deployment_info)


def skip_deploy(**context):
    """Обработка пропуска деплоя"""
    logger.info(f"Модель {MODEL_VERSION} не развернута (плохие метрики)")


def notify_telegram(**context):
    """Отправка уведомления в Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("elegram не настроен — уведомление не отправлено")
        return

    deployment_info = context["ti"].xcom_pull(key="deployment_info", task_ids="deploy_model")
    message = (
        f"Выкатилась новая версия\n"
        f"Версия: **{MODEL_VERSION}**\n"
        f"Дата запуска: {deployment_info['deploy_time']}\n"
        f"Метрики:"
        f"Accuracy: {deployment_info['metrics']['accuracy']}\n"
        f"F1-score: {deployment_info['metrics']['f1_score']}"
    )

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "markdown"
        }
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("Уведомление в Telegram отправлено успешно")
        else:
            logger.info(f"Ошибка Telegram: {response.text}")
    except Exception as e:
        logger.info(f"Не удалось отправить сообщение в Telegram: {e}")


default_args = {
    "owner": "mlops-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ml_retrain_pipeline",
    default_args=default_args,
    description="ML retraining pipeline with Telegram notifications",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "retraining", "notifications"],
) as dag:

    load_data_task = PythonOperator(task_id="load_data", python_callable=load_data)
    train_model_task = PythonOperator(task_id="train_model", python_callable=train_model)
    evaluate_model_task = PythonOperator(task_id="evaluate_model", python_callable=evaluate_model)
    check_quality_task = BranchPythonOperator(task_id="check_quality", python_callable=check_quality)
    deploy_model_task = PythonOperator(task_id="deploy_model", python_callable=deploy_model)
    skip_deploy_task = PythonOperator(task_id="skip_deploy", python_callable=skip_deploy)
    notify_telegram_task = PythonOperator(task_id="notify_telegram", python_callable=notify_telegram, trigger_rule="all_success")
    join_task = DummyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

    load_data_task >> train_model_task >> evaluate_model_task >> check_quality_task
    check_quality_task >> [deploy_model_task, skip_deploy_task]
    deploy_model_task >> notify_telegram_task >> join_task
    skip_deploy_task >> join_task
