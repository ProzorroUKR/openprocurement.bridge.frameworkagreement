# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import logging

import jmespath
from gevent import sleep
from gevent.greenlet import Greenlet
from gevent.queue import Empty
from openprocurement.bridge.basic.interfaces import IFilter
from zope.interface import implementer

from openprocurement.bridge.frameworkagreement.utils import journal_context


logger = logging.getLogger(__name__)
INFINITY = True


@implementer(IFilter)
class CFAUAFilter(Greenlet):

    def __init__(self, conf, input_queue, filtered_queue, db):
        logger.info("Init Close Framework Agreement Filter.")
        Greenlet.__init__(self)
        self.config = conf
        self.cache_db = db
        self.input_queue = input_queue
        self.filtered_queue = filtered_queue
        self.resource = self.config['resource']
        self.resource_id = "{}_ID".format(self.resource[:-1]).upper()
        self.procurement_method_types = tuple(self.config['filter_config']['procurementMethodTypes'])
        self.statuses = tuple(self.config['filter_config']['statuses'])
        self.lot_status = self.config['filter_config']['lot_status']
        self.timeout = self.config['filter_config']['timeout']

    def _run(self):
        while INFINITY:
            if not self.input_queue.empty():
                priority, resource = self.input_queue.get()
            else:
                try:
                    priority, resource = self.input_queue.get(timeout=self.timeout)
                except Empty:
                    sleep(self.timeout)
                    continue

            cached = self.cache_db.get(resource['id'])
            if cached and cached == resource['dateModified']:
                logger.info(
                    "{} {} not modified from last check. Skipping".format(self.resource[:-1].title(), resource['id']),
                    extra=journal_context({"MESSAGE_ID": "SKIPPED"}, params={self.resource_id: resource['id']})
                )
                continue
            if resource['procurementMethodType'] not in self.procurement_method_types:
                logger.info(
                    "Skipping {} {} {}".format(resource['procurementMethodType'], self.resource[:-1], resource['id']),
                    extra=journal_context({"MESSAGE_ID": "SKIPPED"}, params={self.resource_id: resource['id']})
                )
                continue

            if resource['status'] not in self.statuses:
                logger.info(
                    "Skipping {} {} {} {}".format(resource['procurementMethodType'], self.resource[:-1],
                                               resource['status'], resource['id']),
                    extra=journal_context({"MESSAGE_ID": "SKIPPED"}, params={self.resource_id: resource['id']})
                )
                continue

            if 'lots' in resource and not any([lot['status'] == self.lot_status for lot in resource['lots']]):
                logger.info(
                    "Skipping multilot {} {} in status {}".format(self.resource[:-1], resource['id'],
                                                                  resource['status']),
                    extra=journal_context({"MESSAGE_ID": "SKIPPED"}, params={self.resource_id: resource['id']})
                )
                continue

            logger.debug("Put to filtered queue {} {}".format(self.resource[:-1], resource['id']))
            self.filtered_queue.put((priority, resource))


@implementer(IFilter)
class JMESPathFilter(Greenlet):

    def __init__(self, conf, input_queue, filtered_queue, db):
        logger.info("Init Close Framework Agreement JMESPath Filter.")
        Greenlet.__init__(self)
        self.config = conf
        self.cache_db = db
        self.input_queue = input_queue
        self.filtered_queue = filtered_queue
        self.resource = self.config['resource']
        self.resource_id = "{}_ID".format(self.resource[:-1]).upper()
        self.filters = [jmespath.compile(expression['expression'])
                        for expression in self.config['filter_config'].get('filters', [])]
        self.timeout = self.config['filter_config']['timeout']

    def _run(self):
        while INFINITY:
            if not self.input_queue.empty():
                priority, resource = self.input_queue.get()
            else:
                try:
                    priority, resource = self.input_queue.get(timeout=self.timeout)
                except Empty:
                    sleep(self.timeout)
                    continue

            cached = self.cache_db.get(resource['id'])
            if cached and cached == resource['dateModified']:
                logger.info(
                    "{} {} not modified from last check. Skipping".format(self.resource[:-1].title(), resource['id']),
                    extra=journal_context({"MESSAGE_ID": "SKIPPED"}, params={self.resource_id: resource['id']})
                )
                continue

            for re in self.filters:
                if re.search(resource):
                    continue
                else:
                    break
            else:
                logger.debug("Put to filtered queue {} {}".format(self.resource[:-1], resource['id']))
                self.filtered_queue.put((priority, resource))
                continue

            logger.info(
                "Skip {} {}".format(self.resource[:-1], resource['id']), 
                extra=journal_context({"MESSAGE_ID": "SKIPPED"}, params={self.resource_id: resource['id']})
            )
