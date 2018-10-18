# -*- coding: utf-8 -*-
import logging

from openprocurement_client.exceptions import (
    RequestFailed,
    ResourceNotFound,
    ResourceGone
)

from tooz import coordination

from openprocurement.bridge.basic.handlers import HandlerTemplate
from openprocurement.bridge.frameworkagreement.utils import journal_context


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


class AgreementObjectMaker(HandlerTemplate):

    def __init__(self, config, cache_db):
        logger.info("Init Close Framework Agreement UA Handler.")
        self.handler_name = 'handler_cfaua'
        super(AgreementObjectMaker, self).__init__(config, cache_db)
        self.basket = {}
        self.keys_from_tender = ('procuringEntity', )

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
