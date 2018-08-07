# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import logging
import os
from datetime import datetime
from gevent import Greenlet, sleep
from gevent.queue import Empty
from iso8601 import parse_date
from openprocurement.bridge.basic.constants import PROCUREMENT_METHOD_TYPE_HANDLERS as handlers_registry
from openprocurement.bridge.basic.interfaces import IWorker
from openprocurement_client.exceptions import (
    InvalidResponse,
    RequestFailed,
    ResourceNotFound,
    ResourceGone
)
from pytz import timezone
from requests.exceptions import ConnectionError
from time import time
from zope.interface import implementer

from openprocurement.bridge.frameworkagreement.utils import journal_context


logger = logging.getLogger(__name__)
INFINITY = True
TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')


@implementer(IWorker)
class AgreementWorker(Greenlet):

    def __init__(self, api_clients_queue=None, resource_items_queue=None, db=None, config_dict=None,
                 retry_resource_items_queue=None, api_clients_info=None):
        Greenlet.__init__(self)
        logger.info("Init CloseFrameworkAgreement UA Worker")
        self.cache_db = db
        self.main_config = config_dict
        self.config = config_dict['worker_config']
        self.input_resource = self.main_config['resource']
        self.resource = self.main_config['resource']
        self.input_resource_id = "{}_ID".format(self.main_config['resource'][:-1]).upper()
        self.api_clients_queue = api_clients_queue
        self.resource_items_queue = resource_items_queue
        self.retry_resource_items_queue = retry_resource_items_queue
        self.api_clients_info = api_clients_info
        self.start_time = datetime.now()
        self.exit = False

    def add_to_retry_queue(self, resource_item, priority=0, status_code=0):
        retries_count = priority - 1000 if priority >= 1000 else priority
        if retries_count > self.config['retries_count'] and status_code != 429:
            logger.critical(
                '{} {} reached limit retries count {} and droped from retry_queue.'.format(
                    self.resource[:-1].title(), resource_item['id'], self.config['retries_count']),
                extra=journal_context({'MESSAGE_ID': 'dropped_documents'}, {"TENDER_ID": resource_item['id']})
            )
            return
        timeout = 0
        if status_code != 429:
            timeout = self.config['retry_default_timeout'] * retries_count
            priority += 1
        sleep(timeout)
        self.retry_resource_items_queue.put((priority, resource_item))
        logger.info('Put to \'retry_queue\' {}: {}'.format(self.resource[:-1], resource_item['id']),
                    extra={'MESSAGE_ID': 'add_to_retry'})

    def _get_api_client_dict(self):
        if not self.api_clients_queue.empty():
            try:
                api_client_dict = self.api_clients_queue.get(timeout=self.config['queue_timeout'])
            except Empty:
                return None
            if self.api_clients_info[api_client_dict['id']]['drop_cookies']:
                try:
                    api_client_dict['client'].renew_cookies()
                    self.api_clients_info[api_client_dict['id']] = {
                        'drop_cookies': False,
                        'request_durations': {},
                        'request_interval': 0,
                        'avg_duration': 0
                    }
                    api_client_dict['request_interval'] = 0
                    api_client_dict['not_actual_count'] = 0
                    logger.debug('Drop lazy api_client {} cookies'.format(api_client_dict['id']))
                except (Exception, ConnectionError) as e:
                    self.api_clients_queue.put(api_client_dict)
                    logger.debug('PUT API CLIENT: {}'.format(api_client_dict['id']), extra={'MESSAGE_ID': 'put_client'})
                    logger.error('While renewing cookies catch exception: {}'.format(e.message))
                    return None
            logger.debug(
                'GET API CLIENT: {} {} with requests interval: {}'.format(
                    api_client_dict['id'],
                    api_client_dict['client'].session.headers['User-Agent'],
                    api_client_dict['request_interval']
                ),
                extra={'MESSAGE_ID': 'get_client', 'REQUESTS_TIMEOUT': api_client_dict['request_interval']}
            )
            sleep(api_client_dict['request_interval'])
            return api_client_dict
        else:
            return None

    def _get_resource_item_from_public(self, api_client_dict, priority, resource_item):
        try:
            logger.debug('Request interval {} sec. for client {}'.format(
                api_client_dict['request_interval'], api_client_dict['client'].session.headers['User-Agent']),
                extra={'REQUESTS_TIMEOUT': api_client_dict['request_interval']})
            start = time()
            public_resource_item = api_client_dict['client'].get_resource_item(resource_item['id']).get('data')
            self.api_clients_info[api_client_dict['id']]['request_durations'][datetime.now()] = time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] = api_client_dict['request_interval']
            logger.debug('Recieved from API {}: {} {}'.format(self.resource[:-1], public_resource_item['id'],
                                                              public_resource_item['dateModified']))
            if api_client_dict['request_interval'] > 0:
                api_client_dict['request_interval'] -= self.config['client_dec_step_timeout']
            self.api_clients_queue.put(api_client_dict)
            logger.debug('PUT API CLIENT: {}'.format(api_client_dict['id']), extra={'MESSAGE_ID': 'put_client'})
            return public_resource_item
        except ResourceGone:
            self.api_clients_queue.put(api_client_dict)
            logger.debug('PUT API CLIENT: {}'.format(api_client_dict['id']), extra={'MESSAGE_ID': 'put_client'})
            logger.info('{} {} archived.'.format(self.resource[:-1].title(), resource_item['id']))
            return None  # Archived
        except InvalidResponse as e:
            self.api_clients_info[api_client_dict['id']]['request_durations'][datetime.now()] = time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] = api_client_dict['request_interval']
            self.api_clients_queue.put(api_client_dict)
            logger.debug('PUT API CLIENT: {}'.format(api_client_dict['id']), extra={'MESSAGE_ID': 'put_client'})
            logger.error(
                'Error while getting {} {} from public with status code: {}'.format(self.resource[:-1],
                                                                                    resource_item['id'], e.status_code),
                extra={'MESSAGE_ID': 'exceptions'})
            self.add_to_retry_queue(resource_item, priority=priority)
            return None
        except RequestFailed as e:
            self.api_clients_info[api_client_dict['id']]['request_durations'][datetime.now()] = time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] = api_client_dict['request_interval']
            if e.status_code == 429:
                if api_client_dict['request_interval'] > self.config['drop_threshold_client_cookies']:
                    api_client_dict['client'].session.cookies.clear()
                    api_client_dict['request_interval'] = 0
                else:
                    api_client_dict['request_interval'] += self.config['client_inc_step_timeout']
                self.api_clients_queue.put(api_client_dict, timeout=api_client_dict['request_interval'])
                logger.warning('PUT API CLIENT: {} after {} sec.'.format(api_client_dict['id'],
                                                                         api_client_dict['request_interval']),
                               extra={'MESSAGE_ID': 'put_client'})
            else:
                self.api_clients_queue.put(api_client_dict)
                logger.debug('PUT API CLIENT: {}'.format(api_client_dict['id']), extra={'MESSAGE_ID': 'put_client'})
            logger.error(
                'Request failed while getting {} {} from public with status code {}: '.format(
                    self.resource[:-1], resource_item['id'], e.status_code),
                extra={'MESSAGE_ID': 'exceptions'})
            self.add_to_retry_queue(resource_item, priority=priority, status_code=e.status_code)
            return None  # request failed
        except ResourceNotFound as e:
            self.api_clients_info[api_client_dict['id']]['request_durations'][datetime.now()] = time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] = api_client_dict['request_interval']
            logger.error('Resource not found {} at public: {}. {}'.format(self.resource[:-1],
                                                                          resource_item['id'], e.message),
                         extra={'MESSAGE_ID': 'not_found_docs'})
            api_client_dict['client'].session.cookies.clear()
            logger.debug('Clear client cookies')
            self.api_clients_queue.put(api_client_dict)
            self.add_to_retry_queue(resource_item, priority=priority)
            logger.debug('PUT API CLIENT: {}'.format(api_client_dict['id']), extra={'MESSAGE_ID': 'put_client'})
            return None  # not found
        except Exception as e:
            self.api_clients_info[api_client_dict['id']]['request_durations'][datetime.now()] = time() - start
            self.api_clients_info[api_client_dict['id']]['request_interval'] = api_client_dict['request_interval']
            self.api_clients_queue.put(api_client_dict)
            logger.debug('PUT API CLIENT: {}'.format(api_client_dict['id']), extra={'MESSAGE_ID': 'put_client'})
            logger.error(
                'Error while getting resource item {} {} from public {}: '.format(self.resource[:-1],
                                                                                  resource_item['id'], e.message),
                extra={'MESSAGE_ID': 'exceptions'})
            self.add_to_retry_queue(resource_item, priority=priority)
        return None

    def _get_resource_item_from_queue(self):
        priority = resource_item = None
        if not self.resource_items_queue.empty():
            priority, resource_item = self.resource_items_queue.get(timeout=self.config['queue_timeout'])
            logger.debug('Get {} {} from main queue.'.format(self.input_resource[:-1], resource_item['id']))
        return priority, resource_item

    def log_timeshift(self, resource_item):
        ts = (datetime.now(TZ) - parse_date(resource_item['dateModified'])).total_seconds()
        logger.debug('{} {} timeshift is {} sec.'.format(self.input_resource[:-1], resource_item['id'], ts),
                     extra={'DOCUMENT_TIMESHIFT': ts})

    def _run(self):
        while not self.exit:
            # Try get api client from clients queue
            api_client_dict = self._get_api_client_dict()
            if api_client_dict is None:
                logger.debug('API clients queue is empty.')
                sleep(self.config['worker_sleep'])
                continue

            # Try get item from resource items queue
            priority, resource_item = self._get_resource_item_from_queue()
            if resource_item is None:
                self.api_clients_queue.put(api_client_dict)
                logger.debug('PUT API CLIENT: {}'.format(api_client_dict['id']), extra={'MESSAGE_ID': 'put_client'})
                logger.debug('Resource items queue is empty.')
                sleep(self.config['worker_sleep'])
                continue

            handler = handlers_registry.get(resource_item['procurementMethodType'], '')
            if not handler:
                logger.critical(
                    "Not found handler for procurementMethodType: {}, {} {}".format(
                        resource_item['procurementMethodType'], self.resource[:-1], resource_item['id']
                    ),
                    extra=journal_context({"MESSAGE_ID": "bridge_worker_exception"},
                                          {self.input_resource_id: resource_item['id']})
                )
                continue

            # Try get resource item from public server
            public_resource_item = self._get_resource_item_from_public(api_client_dict, priority, resource_item)
            if public_resource_item is None:
                continue

            try:
                handler.process_resource(public_resource_item)
            except RequestFailed as e:
                logger.error(
                    "Error while processing {} {}: {}".format(self.resource[:-1], resource_item['id'], e.message),
                    extra=journal_context({"MESSAGE_ID": "bridge_worker_exception"},
                                          {self.input_resource_id: resource_item['id']})
                )
                self.add_to_retry_queue(resource_item, priority)
            except Exception as e:
                logger.error(
                    "Error while processing {} {}: {}".format(self.resource[:-1], resource_item['id'], e.message),
                    extra=journal_context({"MESSAGE_ID": "bridge_worker_exception"},
                                          {self.input_resource_id: resource_item['id']})
                )
                self.add_to_retry_queue(resource_item, priority)

    def shutdown(self):
        self.exit = True
        logger.info('CloseFrameworkAgreement Worker complete his job.')
