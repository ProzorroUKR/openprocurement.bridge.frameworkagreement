# -*- coding: utf-8 -*-
import logging
from gevent import sleep
from openprocurement_client.clients import APIResourceClient as APIClient
from openprocurement_client.exceptions import (
    RequestFailed,
    ResourceNotFound,
    ResourceGone
)
from openprocurement.bridge.basic.utils import DataBridgeConfigError
from retrying import retry
from uuid import uuid4
from tooz import coordination

from openprocurement.bridge.frameworkagreement.utils import journal_context, generate_req_id


config = {
    'worker_type': 'contracting',
    'client_inc_step_timeout': 0.1,
    'client_dec_step_timeout': 0.02,
    'drop_threshold_client_cookies': 2,
    'worker_sleep': 5,
    'retry_default_timeout': 3,
    'retries_count': 10,
    'queue_timeout': 3,
    'bulk_save_limit': 100,
    'bulk_save_interval': 3,
    'resources_api_token': '',
    'resources_api_version': '',
    'input_resources_api_server': '',
    'input_public_resources_api_server': '',
    'input_resource': 'tenders',
    'output_resources_api_server': '',
    'output_public_resources_api_server': '',
    'output_resource': 'agreements',
    'handler_cfaua': {
        'resources_api_token': '',
        'output_resources_api_token': 'agreement_token',
        'resources_api_version': '',
        'input_resources_api_token': 'tender_token',
        'input_resources_api_server': '',
        'input_public_resources_api_server': '',
        'input_resource': 'tenders',
        'output_resources_api_server': '',
        'output_public_resources_api_server': '',
        'output_resource': 'agreements'
    }
}

CONFIG_MAPPING = {
    'input_resources_api_token': 'resources_api_token',
    'output_resources_api_token': 'resources_api_token',
    'resources_api_version': 'resources_api_version',
    'input_resources_api_server': 'resources_api_server',
    'input_public_resources_api_server': 'public_resources_api_server',
    'input_resource': 'resource',
    'output_resources_api_server': 'resources_api_server',
    'output_public_resources_api_server': 'public_resources_api_server'
}


logger = logging.getLogger(__name__)


class HandlerTemplate(object):

    def __init__(self, config, cache_db):
        self.cache_db = cache_db
        self.handler_config = config['worker_config'].get(self.handler_name, {})
        self.main_config = config
        self.config_keys = ('input_resources_api_token', 'output_resources_api_token', 'resources_api_version', 'input_resources_api_server',
                            'input_public_resources_api_server', 'input_resource', 'output_resources_api_server',
                            'output_public_resources_api_server', 'output_resource')
        self.validate_and_fix_handler_config()
        self.initialize_clients()

    def validate_and_fix_handler_config(self):
        for key in self.config_keys:
            if key not in self.handler_config:
                self.handler_config[key] = self.main_config['worker_config'].get(key, '')
        for key in CONFIG_MAPPING.keys():
            if not self.handler_config[key]:
                self.handler_config[key] = self.main_config[CONFIG_MAPPING[key]]

        if not self.handler_config['output_resource']:
            raise DataBridgeConfigError("Missing 'output_resource' in handler configuration.")

    def initialize_clients(self):
        self.output_client = self.create_api_client()
        self.public_output_client = self.create_api_client(read_only=True)
        self.input_client = self.create_api_client(input_resource=True)

    def create_api_client(self, input_resource=False, read_only=False):
        client_user_agent = 'contracting_worker' + '/' + uuid4().hex
        timeout = 0.1
        while 1:
            try:
                if input_resource:
                    api_client = APIClient(host_url=self.handler_config['input_resources_api_server'],
                                           user_agent=client_user_agent,
                                           api_version=self.handler_config['resources_api_version'],
                                           key=self.handler_config['input_resources_api_token'],
                                           resource=self.handler_config['input_resource'])
                else:
                    if read_only:
                        api_client = APIClient(host_url=self.handler_config['output_public_resources_api_server'],
                                               user_agent=client_user_agent,
                                               api_version=self.handler_config['resources_api_version'],
                                               key='',
                                               resource=self.handler_config['output_resource'])
                    else:
                        api_client = APIClient(host_url=self.handler_config['output_resources_api_server'],
                                               user_agent=client_user_agent,
                                               api_version=self.handler_config['resources_api_version'],
                                               key=self.handler_config['output_resources_api_token'],
                                               resource=self.handler_config['output_resource'])
                return api_client
            except RequestFailed as e:
                logger.error('Failed start api_client with status code {}'.format(e.status_code),
                             extra={'MESSAGE_ID': 'exceptions'})
                timeout = timeout * 2
                logger.info('create_api_client will be sleep {} sec.'.format(timeout))
                sleep(timeout)
            except Exception as e:
                logger.error('Failed start api client with error: {}'.format(e.message),
                             extra={'MESSAGE_ID': 'exceptions'})
                timeout = timeout * 2
                logger.info('create_api_client will be sleep {} sec.'.format(timeout))
                sleep(timeout)

    def _put_resource_in_cache(self, resource):
        date_modified = self.cache_db.get(resource['id'])
        if not date_modified or date_modified < resource['dateModified']:
            self.cache_db.put(resource['id'], resource['dateModified'])


class AgreementObjectMaker(HandlerTemplate):

    def __init__(self, config, cache_db):
        logger.info("Init Close Framework Agreement UA Handler.")
        self.handler_name = 'handler_cfaua'
        super(AgreementObjectMaker, self).__init__(config, cache_db)
        self.basket = {}
        self.keys_from_tender = ('procuringEntity', )

    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def get_resource_credentials(self, resource_id):
        self.input_client.headers.update({'X-Client-Request-ID': generate_req_id()})
        logger.info(
            "Getting credentials for tender {}".format(resource_id),
            extra=journal_context({"MESSAGE_ID": "databridge_get_credentials"}, {"TENDER_ID": resource_id})
        )
        data = self.input_client.extract_credentials(resource_id)
        logger.info(
            "Got tender {} credentials".format(resource_id),
            extra=journal_context({"MESSAGE_ID": "databridge_got_credentials"}, {"TENDER_ID": resource_id})
        )
        return data

    def fill_agreement(self, agreement, resource):
        credentials_data = self.get_resource_credentials(resource['id'])
        assert 'owner' in credentials_data.get('data', {})
        assert 'tender_token' in credentials_data.get('data', {})
        agreement['agreementType'] = 'cfaua'
        agreement['tender_id'] = resource['id']
        agreement['tender_token'] = credentials_data['data']['tender_token']
        agreement['owner'] = credentials_data['data']['owner']
        for key in self.keys_from_tender:
            agreement[key] = resource[key]
        if 'mode' in resource:
            agreement['mode'] = resource['mode']

    def post_agreement(self, agreement):
        data = {"data": agreement.toDict()}
        logger.info(
            "Creating agreement {} of tender {}".format(agreement['id'], agreement['tender_id']),
            extra=journal_context({"MESSAGE_ID": "agreement_creating"},
                                  {"TENDER_ID": agreement['tender_id'], "AGREEMENENT_ID": agreement['id']})
        )
        self.output_client.create_resource_item(data)

    def process_resource(self, resource):
        if 'agreements' not in resource:
            logger.warn(
                "No agreements found in tender {}".format(resource['id']),
                extra=journal_context({"MESSAGE_ID": "missing_agreements"}, params={"TENDER_ID": resource['id']})
            )
            return

        for agreement in resource['agreements']:
            if agreement['status'] != 'active':
                continue

            try:
                if not self.cache_db.has(agreement['id']):
                    self.public_output_client.get_resource_item(agreement['id'])
                else:
                    logger.info('Agreement {} exist in local db'.format(agreement['id']))
                    self._put_resource_in_cache(resource)
                    continue
            except ResourceNotFound:
                logger.info(
                    'Sync agreement {} of tender {}'.format(agreement['id'], resource['id']),
                    extra=journal_context({"MESSAGE_ID": 'got_agreement_for_sync'},
                                          {"AGREEMENT_ID": agreement['id'], "TENDER_ID": resource['id']})
                )
            except ResourceGone:
                logger.info(
                    "Sync agreement {} of tender {} has been archived".format(agreement['id'], resource['id']),
                    extra=journal_context({"MESSAGE_ID": 'skip_agreement'},
                                          params=({"TENDER_ID": resource['id'], "AGREEMENT_ID": agreement['id']}))
                )
                self._put_resource_in_cache(resource)
                continue
            else:
                self.cache_db.put(agreement['id'], True)
                logger.info(
                    "Agreement {} already exist".format(agreement['id']),
                    extra=journal_context({"MESSAGE_ID": "skip_agreement"},
                                          params=({"TENDER_ID": resource['id'], "AGREEMENT_ID": agreement['id']}))
                )
                self._put_resource_in_cache(resource)
                continue

            self.fill_agreement(agreement, resource)
            self.post_agreement(agreement)
            self.cache_db.put(agreement['id'], True)
        self._put_resource_in_cache(resource)


class CFASelectionUAHandler(HandlerTemplate):

    def __init__(self, config, cache_db):
        logger.info("init CFA Selection UA Handler.")
        self.handler_name = 'handler_cfaselectionua'
        super(CFASelectionUAHandler, self).__init__(config, cache_db)
        coordinator_config = config.get('coordinator_config', {})
        self.coordinator = coordination.get_coordinator(coordinator_config.get('connection_url', 'redis://'),
                                                        coordinator_config.get('coordinator_name', 'bridge'))
        self.coordinator.start(start_heart=True)

    def process_resource(self, resource):
        lock = self.coordinator.get_lock(resource['id'])
        if lock._client.exists(lock._name):
            logger.info(
                "Tender {} processing by another worker.".format(resource['id']),
                extra=journal_context({"MESSAGE_ID": 'tender_already_in_process'},
                                      params={"TENDER_ID": resource['id']}))
            return
        with lock:
            for agreement in resource['agreements']:
                agreement_data = self.input_client.get_resource_item(agreement['id'])
                logger.info(
                    "Received agreement data {}".format(agreement['id']),
                    extra=journal_context({"MESSAGE_ID": 'received_agreement_data'},
                                          params={"TENDER_ID": resource['id'], "AGREEMENT_ID": agreement['id']}))
                agreement_data['data'].pop('id')

                # Fill agreement data
                logger.info(
                    "Patch tender agreement {}".format(agreement['id']),
                    extra=journal_context({"MESSAGE_ID": 'patch_agreement_data'},
                                          params={"TENDER_ID": resource['id'], "AGREEMENT_ID": agreement['id']}))
                self.output_client.patch_resource_item_subitem(
                    resource['id'], agreement_data, 'agreements', subitem_id=agreement['id']
                )

            # Swith tender status
            response = self.output_client.patch_resource_item(resource['id'], {'data': {'status': 'active.enquiries'}})
            logger.info(
                "Switch tender {} status to {}".format(resource['id'], response['data']['status']),
                extra=journal_context({"MESSAGE_ID": 'patch_tender_status'}, params={"TENDER_ID": resource['id']}))
