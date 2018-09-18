# -*- coding: utf-8 -*-
import datetime
import unittest
import uuid
import logging
import iso8601
from gevent import sleep, idle
from gevent.queue import Queue, PriorityQueue, Empty
from mock import MagicMock, patch, call
from munch import munchify
from openprocurement_client.exceptions import (
    InvalidResponse,
    RequestFailed,
    ResourceNotFound as RNF,
    ResourceGone
)
from openprocurement.bridge.frameworkagreement.workers import AgreementWorker
from openprocurement.bridge.frameworkagreement.workers import logger

logger.setLevel(logging.DEBUG)


class TestResourceAgreementWorker(unittest.TestCase):
    worker_config = {
        'worker_config': {
            'worker_type': 'basic_couchdb',
            'client_inc_step_timeout': 0.1,
            'client_dec_step_timeout': 0.02,
            'drop_threshold_client_cookies': 1.5,
            'worker_sleep': 5,
            'retry_default_timeout': 0.5,
            'retries_count': 2,
            'queue_timeout': 3,
            'bulk_save_limit': 100,
            'bulk_save_interval': 3
        },
        'storage_config': {
            # required for databridge
            "storage_type": "couchdb",  # possible values ['couchdb', 'elasticsearch']
            # arguments for storage configuration
            "host": "localhost",
            "port": 5984,
            "user": "",
            "password": "",
            "db_name": "basic_bridge_db",
            "bulk_query_interval": 3,
            "bulk_query_limit": 100,
        },
        'filter_type': 'basic_couchdb',
        'retrievers_params': {
            'down_requests_sleep': 5,
            'up_requests_sleep': 1,
            'up_wait_sleep': 30,
            'queue_size': 1001
        },
        'extra_params': {
            "mode": "_all_",
            "limit": 1000
        },
        'bridge_mode': 'basic',
        'resources_api_server': 'http://localhost:1234',
        'resources_api_version': "0",
        'resources_api_token': '',
        'public_resources_api_server': 'http://localhost:1234',
        'resource': 'tenders',
        'workers_inc_threshold': 75,
        'workers_dec_threshold': 35,
        'workers_min': 1,
        'workers_max': 3,
        'filter_workers_count': 1,
        'retry_workers_min': 1,
        'retry_workers_max': 2,
        'retry_resource_items_queue_size': -1,
        'watch_interval': 10,
        'user_agent': 'bridge.basic',
        'resource_items_queue_size': 10000,
        'input_queue_size': 10000,
        'resource_items_limit': 1000,
        'queues_controller_timeout': 60,
        'perfomance_window': 300
    }

    def tearDown(self):
        self.worker_config['resource'] = 'tenders'
        self.worker_config['client_inc_step_timeout'] = 0.1
        self.worker_config['client_dec_step_timeout'] = 0.02
        self.worker_config['drop_threshold_client_cookies'] = 1.5
        self.worker_config['worker_sleep'] = 0.03
        self.worker_config['retry_default_timeout'] = 0.01
        self.worker_config['retries_count'] = 2
        self.worker_config['queue_timeout'] = 0.03

    def test_init(self):
        worker = AgreementWorker('api_clients_queue', 'resource_items_queue', 'db',
                                 {'worker_config': {'bulk_save_limit': 1, 'bulk_save_interval': 1},
                                  'resource': 'tenders'}, 'retry_resource_items_queue')
        self.assertEqual(worker.api_clients_queue, 'api_clients_queue')
        self.assertEqual(worker.resource_items_queue, 'resource_items_queue')
        self.assertEqual(worker.cache_db, 'db')
        self.assertEqual(worker.config, {'bulk_save_limit': 1, 'bulk_save_interval': 1})
        self.assertEqual(worker.retry_resource_items_queue, 'retry_resource_items_queue')
        self.assertEqual(worker.input_resource_id, 'TENDER_ID')
        self.assertEqual((worker.api_clients_info, worker.exit), (None, False))

    @patch('openprocurement.bridge.frameworkagreement.workers.logger')
    def test_add_to_retry_queue(self, mocked_logger):
        retry_items_queue = PriorityQueue()
        worker = AgreementWorker(config_dict=self.worker_config, retry_resource_items_queue=retry_items_queue)
        resource_item = {'id': uuid.uuid4().hex}
        priority = 1000
        self.assertEqual(retry_items_queue.qsize(), 0)

        # Add to retry_resource_items_queue
        worker.add_to_retry_queue(resource_item, priority=priority)

        self.assertEqual(retry_items_queue.qsize(), 1)
        priority, retry_resource_item = retry_items_queue.get()
        self.assertEqual((priority, retry_resource_item), (1001, resource_item))

        resource_item = {'id': 0}
        # Add to retry_resource_items_queue with status_code '429'
        worker.add_to_retry_queue(resource_item, priority, status_code=429)
        self.assertEqual(retry_items_queue.qsize(), 1)
        priority, retry_resource_item = retry_items_queue.get()
        self.assertEqual((priority, retry_resource_item), (1001, resource_item))

        priority = 1002
        worker.add_to_retry_queue(resource_item, priority=priority)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(retry_items_queue.qsize(), 1)
        priority, retry_resource_item = retry_items_queue.get()
        self.assertEqual((priority, retry_resource_item), (1003, resource_item))

        worker.add_to_retry_queue(resource_item, priority=priority)
        self.assertEqual(retry_items_queue.qsize(), 0)
        mocked_logger.critical.assert_called_once_with(
            'Tender {} reached limit retries count {} and droped from '
            'retry_queue.'.format(resource_item['id'], worker.config['retries_count']),
            extra={'MESSAGE_ID': 'dropped_documents', 'JOURNAL_TENDER_ID': resource_item['id']}
        )
        del worker

    def test__get_api_client_dict(self):
        api_clients_queue = Queue()
        client = MagicMock()
        client_dict = {
            'id': uuid.uuid4().hex,
            'client': client,
            'request_interval': 0
        }
        client_dict2 = {
            'id': uuid.uuid4().hex,
            'client': client,
            'request_interval': 0
        }
        api_clients_queue.put(client_dict)
        api_clients_queue.put(client_dict2)
        api_clients_info = {
            client_dict['id']: {
                'drop_cookies': False,
                'not_actual_count': 5,
                'request_interval': 3
            },
            client_dict2['id']: {
                'drop_cookies': True,
                'not_actual_count': 3,
                'request_interval': 2
            }
        }

        # Success test
        worker = AgreementWorker(api_clients_queue=api_clients_queue, config_dict=self.worker_config,
                                 api_clients_info=api_clients_info)
        self.assertEqual(worker.api_clients_queue.qsize(), 2)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, client_dict)

        # Get lazy client
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client['not_actual_count'], 0)
        self.assertEqual(api_client['request_interval'], 0)

        # Empty queue test
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, None)

        # Exception when try renew cookies
        client.renew_cookies.side_effect = Exception('Can\'t renew cookies')
        worker.api_clients_queue.put(client_dict2)
        api_clients_info[client_dict2['id']]['drop_cookies'] = True
        api_client = worker._get_api_client_dict()
        self.assertIs(api_client, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(worker.api_clients_queue.get(), client_dict2)

        # Get api_client with raise Empty exception
        api_clients_queue.put(client_dict2)
        api_clients_queue.get = MagicMock(side_effect=Empty)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, None)
        del worker

    def test__get_resource_item_from_queue(self):
        items_queue = PriorityQueue()
        item = (1, {'id': uuid.uuid4().hex})
        items_queue.put(item)

        # Success test
        worker = AgreementWorker(resource_items_queue=items_queue, config_dict=self.worker_config)
        self.assertEqual(worker.resource_items_queue.qsize(), 1)
        priority, resource_item = worker._get_resource_item_from_queue()
        self.assertEqual((priority, resource_item), item)
        self.assertEqual(worker.resource_items_queue.qsize(), 0)

        # Empty queue test
        priority, resource_item = worker._get_resource_item_from_queue()
        self.assertEqual(resource_item, None)
        self.assertEqual(priority, None)
        del worker

    @patch('openprocurement_client.client.TendersClient')
    def test__get_resource_item_from_public(self, mock_api_client):
        resource_item = {'id': uuid.uuid4().hex}
        resource_item_id = uuid.uuid4().hex
        priority = 1

        api_clients_queue = Queue()
        client_dict = {
            'id': uuid.uuid4().hex,
            'request_interval': 0.02,
            'client': mock_api_client
        }
        api_clients_queue.put(client_dict)
        api_clients_info = {client_dict['id']: {'drop_cookies': False, 'request_durations': {}}}
        retry_queue = PriorityQueue()
        return_dict = {
            'data': {
                'id': resource_item_id,
                'dateModified': datetime.datetime.utcnow().isoformat()
            }
        }
        mock_api_client.get_resource_item.return_value = return_dict
        worker = AgreementWorker(api_clients_queue=api_clients_queue, config_dict=self.worker_config,
                                 retry_resource_items_queue=retry_queue, api_clients_info=api_clients_info)

        # Success test
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client['request_interval'], 0.02)
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(
            api_client, priority, resource_item
        )
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 0)
        self.assertEqual(public_item, return_dict['data'])

        # InvalidResponse
        mock_api_client.get_resource_item.side_effect = InvalidResponse('invalid response')
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, priority, resource_item)
        self.assertEqual(public_item, None)
        sleep(worker.config['retry_default_timeout'] * 1)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 1)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)

        # RequestFailed status_code=429
        mock_api_client.get_resource_item.side_effect = RequestFailed(munchify({'status_code': 429}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        self.assertEqual(api_client['request_interval'], 0)
        public_item = worker._get_resource_item_from_public(api_client, priority, resource_item)
        self.assertEqual(public_item, None)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 2)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        self.assertEqual(api_client['request_interval'],
                         worker.config['client_inc_step_timeout'])

        # RequestFailed status_code=429 with drop cookies
        api_client['request_interval'] = 2
        public_item = worker._get_resource_item_from_public(api_client, priority, resource_item)
        sleep(api_client['request_interval'])
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 3)

        # RequestFailed with status_code not equal 429
        mock_api_client.get_resource_item.side_effect = RequestFailed(
            munchify({'status_code': 404}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, priority, resource_item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 4)

        # ResourceNotFound
        mock_api_client.get_resource_item.side_effect = RNF(munchify({'status_code': 404}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, priority, resource_item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 5)

        # ResourceGone
        mock_api_client.get_resource_item.side_effect = ResourceGone(munchify({'status_code': 410}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._get_resource_item_from_public(api_client, priority, resource_item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 5)

        # Exception
        api_client = worker._get_api_client_dict()
        mock_api_client.get_resource_item.side_effect = Exception('text except')
        public_item = worker._get_resource_item_from_public(api_client, priority, resource_item)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 6)

        del worker

    def test_shutdown(self):
        worker = AgreementWorker('api_clients_queue', 'resource_items_queue', 'db',
                                 {'worker_config': {'bulk_save_limit': 1, 'bulk_save_interval': 1},
                                  'resource': 'tenders'}, 'retry_resource_items_queue')
        self.assertEqual(worker.exit, False)
        worker.shutdown()
        self.assertEqual(worker.exit, True)

    def up_worker(self):
        worker_thread = AgreementWorker.spawn(
            resource_items_queue=self.queue,
            retry_resource_items_queue=self.retry_queue,
            api_clients_info=self.api_clients_info,
            api_clients_queue=self.api_clients_queue,
            config_dict=self.worker_config, db=self.db)
        idle()
        worker_thread.shutdown()
        sleep(3)

    @patch('openprocurement.bridge.frameworkagreement.workers.handlers_registry')
    @patch('openprocurement.bridge.frameworkagreement.workers.AgreementWorker._get_resource_item_from_public')
    @patch('openprocurement.bridge.frameworkagreement.workers.logger')
    def test__run(self, mocked_logger, mock_get_from_public, mock_registry):
        self.queue = Queue()
        self.retry_queue = Queue()
        self.api_clients_queue = Queue()
        queue_item = (1, {'id': uuid.uuid4().hex, 'procurementMethodType': 'closeFrameworkAgreementUA'})
        doc = {
            'id': queue_item[1],
            '_rev': '1-{}'.format(uuid.uuid4().hex),
            'dateModified': datetime.datetime.utcnow().isoformat(),
            'doc_type': 'Tender'
        }
        client = MagicMock()
        api_client_dict = {
            'id': uuid.uuid4().hex,
            'client': client,
            'request_interval': 0
        }
        client.session.headers = {'User-Agent': 'Test-Agent'}
        self.api_clients_info = {
            api_client_dict['id']: {'drop_cookies': False, 'request_durations': []}
        }
        self.db = MagicMock()
        worker = AgreementWorker(
            api_clients_queue=self.api_clients_queue,
            resource_items_queue=self.queue,
            retry_resource_items_queue=self.retry_queue,
            db=self.db, api_clients_info=self.api_clients_info,
            config_dict=self.worker_config
        )
        worker.exit = MagicMock()
        worker.exit.__nonzero__.side_effect = [False, True]

        # Try get api client from clients queue
        self.assertEqual(self.queue.qsize(), 0)
        worker._run()
        self.assertEqual(self.queue.qsize(), 0)
        mocked_logger.critical.assert_called_once_with(
            'API clients queue is empty.')

        # Try get item from resource items queue with no handler
        self.api_clients_queue.put(api_client_dict)
        worker.exit.__nonzero__.side_effect = [False, True]
        mock_registry.get.return_value = ''
        self.queue.put(queue_item)
        mock_get_from_public.return_value = doc
        worker._run()
        self.assertEqual(
            mocked_logger.critical.call_args_list,
            [
                call('API clients queue is empty.'),
                call(
                    'Not found handler for procurementMethodType: {}, {} {}'.format(
                        doc['id']['procurementMethodType'],
                        self.worker_config['resource'][:-1],
                        doc['id']['id']
                    ),
                    extra={'JOURNAL_TENDER_ID': doc['id']['id'], 'MESSAGE_ID': 'bridge_worker_exception'}
                )
            ]
        )

        # Try get item from resource items queue
        self.api_clients_queue.put(api_client_dict)
        worker.exit.__nonzero__.side_effect = [False, True]
        handler_mock = MagicMock()
        handler_mock.process_resource.return_value = None
        mock_registry.return_value = {'closeFrameworkAgreementUA': handler_mock}
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[2:],
            [
                call(
                    'GET API CLIENT: {} {} with requests interval: {}'.format(
                        api_client_dict['id'],
                        api_client_dict['client'].session.headers['User-Agent'],
                        api_client_dict['request_interval']
                    ),
                    extra={'REQUESTS_TIMEOUT': 0, 'MESSAGE_ID': 'get_client'}
                ),
                call('PUT API CLIENT: {}'.format(api_client_dict['id']),
                     extra={'MESSAGE_ID': 'put_client'}),
                call('Resource items queue is empty.')
            ]
        )

        # Try get resource item from local storage
        self.queue.put(queue_item)
        mock_get_from_public.return_value = doc
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[5:],
            [
                call(
                    'GET API CLIENT: {} {} with requests interval: {}'.format(
                        api_client_dict['id'],
                        api_client_dict['client'].session.headers['User-Agent'],
                        api_client_dict['request_interval']
                    ),
                    extra={'REQUESTS_TIMEOUT': 0, 'MESSAGE_ID': 'get_client'}
                ),
                call('Get tender {} from main queue.'.format(doc['id']['id']))
            ]
        )

        # Try get local_resource_item with Exception
        self.api_clients_queue.put(api_client_dict)
        self.queue.put(queue_item)
        mock_get_from_public.return_value = doc
        self.db.get.side_effect = [Exception('Database Error')]
        worker.exit.__nonzero__.side_effect = [False, True]
        worker._run()
        self.assertEqual(
            mocked_logger.debug.call_args_list[7:],
            [
                call(
                    'GET API CLIENT: {} {} with requests interval: {}'.format(
                        api_client_dict['id'],
                        api_client_dict['client'].session.headers['User-Agent'],
                        api_client_dict['request_interval']
                    ),
                    extra={'REQUESTS_TIMEOUT': 0, 'MESSAGE_ID': 'get_client'}
                ),
                call('Get tender {} from main queue.'.format(doc['id']['id']))
            ]
        )

        # Try process resource with Exception
        self.api_clients_queue.put(api_client_dict)
        self.queue.put(queue_item)
        mock_get_from_public.return_value = doc
        worker.exit.__nonzero__.side_effect = [False, True]

        mock_handler = MagicMock()
        mock_handler.process_resource.side_effect = (RequestFailed(),)
        mock_registry.get.return_value = mock_handler

        worker._run()
        self.assertEqual(
            mocked_logger.error.call_args_list,
            [
                call(
                    'Error while processing {} {}: {}'.format(
                        self.worker_config['resource'][:-1],
                        doc['id']['id'],
                        'Not described error yet.'
                    ),
                    extra={'JOURNAL_TENDER_ID': doc['id']['id'], 'MESSAGE_ID': 'bridge_worker_exception'}
                )
            ]
        )
        check_queue_item = (queue_item[0] + 1, queue_item[1])  # priority is increased
        self.assertEquals(self.retry_queue.get(), check_queue_item)

        # Try process resource with Exception
        self.api_clients_queue.put(api_client_dict)
        self.queue.put(queue_item)
        mock_get_from_public.return_value = doc
        worker.exit.__nonzero__.side_effect = [False, True]

        mock_handler = MagicMock()
        mock_handler.process_resource.side_effect = (Exception(),)
        mock_registry.get.return_value = mock_handler
        worker._run()

        self.assertEqual(
            mocked_logger.error.call_args_list[1:],
            [
                call(
                    'Error while processing {} {}: {}'.format(
                        self.worker_config['resource'][:-1],
                        doc['id']['id'],
                        ''
                    ),
                    extra={'JOURNAL_TENDER_ID': doc['id']['id'], 'MESSAGE_ID': 'bridge_worker_exception'}
                )
            ]
        )
        check_queue_item = (queue_item[0] + 1, queue_item[1])  # priority is increased
        self.assertEquals(self.retry_queue.get(), check_queue_item)

        #  No resource item
        self.api_clients_queue.put(api_client_dict)
        self.queue.put(queue_item)
        mock_get_from_public.return_value = None
        worker.exit.__nonzero__.side_effect = [False, True]

        mock_handler = MagicMock()
        mock_handler.process_resource.side_effect = (Exception(),)
        mock_registry.get.return_value = mock_handler
        worker._run()

        self.assertEquals(self.queue.empty(), True)
        self.assertEquals(self.retry_queue.empty(), True)

    @patch('openprocurement.bridge.frameworkagreement.workers.datetime')
    @patch('openprocurement.bridge.frameworkagreement.workers.logger')
    def test_log_timeshift(self, mocked_logger, mocked_datetime):
        worker = AgreementWorker('api_clients_queue', 'resource_items_queue', 'db',
                                 {'worker_config': {'bulk_save_limit': 1, 'bulk_save_interval': 1},
                                  'resource': 'tenders'}, 'retry_resource_items_queue')

        time_var = datetime.datetime.now(iso8601.UTC)

        mocked_datetime.now.return_value = time_var
        resource_item = {'id': '0' * 32,
                         'dateModified': time_var.isoformat()}
        worker.log_timeshift(resource_item)

        self.assertEqual(
            mocked_logger.debug.call_args_list,
            [
                call(
                    '{} {} timeshift is {} sec.'.format(
                        self.worker_config['resource'][:-1],
                        resource_item['id'],
                        0.0
                    ),
                    extra={'DOCUMENT_TIMESHIFT': 0.0}
                )
            ]
        )


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestResourceAgreementWorker))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
