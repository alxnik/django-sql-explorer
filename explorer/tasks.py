from datetime import date, datetime, timedelta
import random
import string

from django.core.mail import EmailMessage
from django.core.cache import cache
from django.db import DatabaseError

from explorer import app_settings
from explorer.exporters import get_exporter_class
from explorer.models import Query, QueryLog

if app_settings.ENABLE_TASKS:
    from celery import task
    from celery.utils.log import get_task_logger
    from explorer.utils import s3_upload
    logger = get_task_logger(__name__)
else:
    from explorer.utils import noop_decorator as task
    import logging
    logger = logging.getLogger(__name__)

def send_email(query_id, email_address_list):
    logger.info("Sending email for query %s..." % query_id)
    q = Query.objects.get(pk=query_id)
    exporter = get_exporter_class('csv')(q)

    try:
        subj = 'Report "%s" is ready' % q.title
        if app_settings.EMAIL_SAVE_TO_S3:
            url = s3_upload('%s.csv' % random_part, exporter.get_file_output())
            msg = 'Download results:\n%s' % url
            attachment_data = None
        else:
            msg = 'Results in attachment:\n'
            attachment_data = exporter.get_output()

    except DatabaseError as e:
        subj = 'Error running report %s' % q.title
        msg = 'Error: %s\nPlease contact an administrator' %  e
        logger.warning('%s: %s' % (subj, e))

    email = EmailMessage(subj, msg, app_settings.FROM_EMAIL, email_address_list)
    if attachment_data:
        email.attach('%s.%s.csv' % (datetime.now().strftime("%Y-%d-%m.%H-%M"), q.title), attachment_data, 'text/csv')

    email.send()



@task
def execute_query(query_id, email_address):
    send_email(query_id, [email_address])


@task
def snapshot_query(query_id):
    try:
        logger.info("Starting snapshot for query %s..." % query_id)
        q = Query.objects.get(pk=query_id)
        exporter = get_exporter_class('csv')(q)
        k = 'query-%s/snap-%s.csv' % (q.id, date.today().strftime('%Y%m%d-%H:%M:%S'))
        logger.info("Uploading snapshot for query %s as %s..." % (query_id, k))
        url = s3_upload(k, exporter.get_file_output())
        logger.info("Done uploading snapshot for query %s. URL: %s" % (query_id, url))
    except Exception as e:
        logger.warning("Failed to snapshot query %s (%s). Retrying..." % (query_id, e))
        snapshot_query.retry()


@task
def snapshot_queries():
    logger.info("Starting query snapshots...")
    qs = Query.objects.filter(snapshot=True).values_list('id', flat=True)
    logger.info("Found %s queries to snapshot. Creating snapshot tasks..." % len(qs))
    for qid in qs:
        snapshot_query.delay(qid)
    logger.info("Done creating tasks.")


@task
def truncate_querylogs(days):
    qs = QueryLog.objects.filter(run_at__lt=datetime.now() - timedelta(days=days))
    logger.info('Deleting %s QueryLog objects older than %s days.' % (qs.count, days))
    qs.delete()
    logger.info('Done deleting QueryLog objects.')


@task
def build_schema_cache_async(connection_alias):
    from .schema import build_schema_info, connection_schema_cache_key
    ret = build_schema_info(connection_alias)
    cache.set(connection_schema_cache_key(connection_alias), ret)
    return ret
