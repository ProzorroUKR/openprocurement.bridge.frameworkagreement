import unittest

from copy import deepcopy
from datetime import datetime
from mock import patch, MagicMock, call
from munch import munchify

from openprocurement_client.exceptions import (
    RequestFailed,
    ResourceNotFound,
    ResourceGone
)

from openprocurement.bridge.basic.utils import DataBridgeConfigError
from openprocurement.bridge.frameworkagreement.tests.base import AdaptiveCache
from openprocurement.bridge.frameworkagreement.handlers import AgreementObjectMaker


class TestAgreementObjectMaker(unittest.TestCase):
    config = {'worker_config': {'handler_cfaua': {
        'resources_api_token': 'resources_api_token',
        'resources_api_version': 'resources_api_version',
        'input_resources_api_server': 'resources_api_server',
        'input_publice_resources_api_server': 'public_resources_api_server',
        'input_resource': 'resource',
        'output_resources_api_server': 'resources_api_server',
        'output_public_resources_api_server': 'public_resources_api_server',
        'output_resource': 'output_resource'
    }}}

    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_init(self, mocked_logger, mocked_client):
        handler = AgreementObjectMaker(self.config, 'cache_db')

        self.assertEquals(handler.cache_db, 'cache_db')
        self.assertEquals(handler.handler_config, self.config['worker_config']['handler_cfaua'])
        self.assertEquals(handler.main_config, self.config)
        self.assertEquals(handler.config_keys,
                          ('resources_api_token', 'resources_api_version', 'input_resources_api_server',
                           'input_publice_resources_api_server', 'input_resource', 'output_resources_api_server',
                           'output_public_resources_api_server', 'output_resource')
                          )
        self.assertEquals(handler.keys_from_tender, ('procuringEntity',))
        mocked_logger.info.assert_called_once_with('Init Close Framework Agreement UA Handler.')

    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_validate_and_fix_handler_config(self, mocked_logger, mocked_client):
        temp_comfig = deepcopy(self.config)
        temp_comfig['resource'] = temp_comfig['worker_config']['handler_cfaua']['input_resource']
        del temp_comfig['worker_config']['handler_cfaua']['input_resource']

        handler = AgreementObjectMaker(temp_comfig, 'cache_db')

        self.assertEquals(handler.handler_config['input_resource'], 'resource')

        temp_comfig = deepcopy(self.config)
        del temp_comfig['worker_config']['handler_cfaua']['output_resource']

        with self.assertRaises(DataBridgeConfigError) as e:
            handler = AgreementObjectMaker(temp_comfig, 'cache_db')
            self.assertEquals(e.message, "Missing 'output_resource' in handler configuration.")

    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_initialize_clients(self, mocked_logger, mocked_client):
        handler = AgreementObjectMaker(self.config, 'cache_db')
        self.assertEquals(isinstance(handler.output_client, MagicMock), True)
        self.assertEquals(isinstance(handler.public_output_client, MagicMock), True)
        self.assertEquals(isinstance(handler.input_client, MagicMock), True)

    @patch('openprocurement.bridge.frameworkagreement.handlers.sleep')
    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_create_api_client(self, mocked_logger, mocked_client, mocked_sleep):
        handler = AgreementObjectMaker(self.config, 'cache_db')

        # emulate RequestFailed
        mocked_client.side_effect = (RequestFailed(),)
        mocked_sleep.side_effect = (KeyboardInterrupt(),)

        with self.assertRaises(KeyboardInterrupt) as e:
            handler.create_api_client()
        self.assertEquals(
            mocked_logger.error.call_args_list,
            [
                call(
                    'Failed start api_client with status code {}'.format(None),
                    extra={'MESSAGE_ID': 'exceptions'}
                )
            ]
        )
        self.assertEquals(
            mocked_logger.info.call_args_list[1:], [call('create_api_client will be sleep {} sec.'.format(0.2))]
        )

        # emulate Exception
        mocked_client.side_effect = (Exception(),)
        mocked_sleep.side_effect = (KeyboardInterrupt(),)

        with self.assertRaises(KeyboardInterrupt) as e:
            handler.create_api_client()
        self.assertEquals(
            mocked_logger.error.call_args_list[1:],
            [
                call(
                    'Failed start api client with error: {}'.format(''),
                    extra={'MESSAGE_ID': 'exceptions'}
                )
            ]
        )
        self.assertEquals(
            mocked_logger.info.call_args_list[2:], [call('create_api_client will be sleep {} sec.'.format(0.2))]
        )

    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_get_resource_credentials(self, mocked_logger, mocked_client):
        handler = AgreementObjectMaker(self.config, 'cache_db')

        resource_id = 'resource_id'
        result_data = {'owner': 'owner', 'tender_token': 'tender_token'}

        input_mock = MagicMock()
        input_mock.extract_credentials.return_value = result_data
        handler.input_client = input_mock

        result = handler.get_resource_credentials(resource_id)
        self.assertEquals(result_data, result)
        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [
                call(
                    'Getting credentials for tender {}'.format(resource_id),
                    extra={'MESSAGE_ID': 'databridge_get_credentials', 'JOURNAL_TENDER_ID': resource_id}
                ),
                call(
                    'Got tender {} credentials'.format(resource_id),
                    extra={'MESSAGE_ID': 'databridge_got_credentials', 'JOURNAL_TENDER_ID': resource_id}
                )
            ]
        )

    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    def test_fill_agreement(self, mocked_client):
        handler = AgreementObjectMaker(self.config, 'cache_db')

        credentials_mock = MagicMock()
        credentials = {'data': {'owner': 'owner', 'tender_token': 'tender_token'}}
        credentials_mock.return_value = credentials
        handler.get_resource_credentials = credentials_mock

        resource = {'id': 'tender_id', 'mode': 'test', 'procuringEntity': 'procuringEntity'}
        agreement = {}
        handler.fill_agreement(agreement, resource)

        self.assertEquals(agreement['agreementType'], 'cfaua')
        self.assertEquals(agreement['tender_id'], resource['id'])
        self.assertEquals(agreement['tender_token'], credentials['data']['tender_token'])
        self.assertEquals(agreement['owner'], credentials['data']['owner'])
        self.assertEquals(agreement['procuringEntity'], resource['procuringEntity'])

    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    def test__put_resource_in_cache(self, mocked_client):
        cache_db = AdaptiveCache({'0' * 32: datetime.now()})
        handler = AgreementObjectMaker(self.config, cache_db)

        new_date = datetime.now()
        resource = {'id': '0' * 32, 'dateModified': new_date}

        handler._put_resource_in_cache(resource)

        self.assertEquals(cache_db.get(resource['id']), new_date)

    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_post_agreement(self, mocked_logger, mocked_client):
        handler = AgreementObjectMaker(self.config, 'cache_db')
        handler.output_client = MagicMock()

        agreement = munchify({'tender_id': 'tender_id', 'id': 'id'})
        handler.post_agreement(agreement)
        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [
                call(
                    'Creating agreement {} of tender {}'.format(agreement['id'], agreement['tender_id']),
                    extra={'JOURNAL_TENDER_ID': agreement['tender_id'], 'MESSAGE_ID': 'agreement_creating',
                           'JOURNAL_AGREEMENENT_ID': 'id'}
                )
            ]
        )
        handler.output_client.create_resource_item.assert_called_with({'data': agreement.toDict()})

    @patch('openprocurement.bridge.frameworkagreement.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_process_resource(self, mocked_logger, mocked_client):
        cache_db = AdaptiveCache({'0' * 32: datetime.now()})
        handler = AgreementObjectMaker(self.config, cache_db)

        # test no agreements
        resource = {'id': '0' * 32}
        handler.process_resource(resource)
        self.assertEquals(
            mocked_logger.warn.call_args_list,
            [
                call(
                    'No agreements found in tender {}'.format(resource['id']),
                    extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': 'missing_agreements'}
                )
            ]
        )

        # not active agreement
        resource['id'] = '1' * 32
        resource['dateModified'] = datetime.now()
        resource['agreements'] = [{'status': 'cancelled', 'id': '2' * 32}]
        handler.process_resource(resource)
        self.assertIn(resource['id'], cache_db)

        # agreement exist in local db
        resource['id'] = '3' * 32
        resource['agreements'] = [{'status': 'active', 'id': '4' * 32}]
        cache_db.put('4' * 32, datetime.now())

        handler.process_resource(resource)

        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [
                call(
                    'Agreement {} exist in local db'.format(resource['agreements'][0]['id'])
                )
            ]
        )
        self.assertIn(resource['id'], cache_db)

        # not in local db
        resource['id'] = '5' * 32
        resource['agreements'] = [{'status': 'active', 'id': '6' * 32}]

        handler.process_resource(resource)

        handler.public_output_client.get_resource_item.assert_called_with(resource['agreements'][0]['id'])
        self.assertIn(resource['agreements'][0]['id'], cache_db)
        self.assertEquals(cache_db[resource['agreements'][0]['id']], True)
        self.assertEquals(
            mocked_logger.info.call_args_list[2:],
            [
                call(
                    'Agreement {} already exist'.format(resource['agreements'][0]['id']),
                    extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': 'skip_agreement',
                           'JOURNAL_AGREEMENT_ID': resource['agreements'][0]['id']}
                )
            ]
        )

        # Resource not found, agreement should be created
        resource['id'] = '7' * 32
        resource['agreements'] = [{'status': 'active', 'id': '8' * 32}]
        e = ResourceNotFound()
        handler.public_output_client.get_resource_item = MagicMock(side_effect=e)
        handler.fill_agreement = MagicMock()
        handler.post_agreement = MagicMock()

        handler.process_resource(resource)

        self.assertIn(resource['id'], cache_db)
        self.assertEquals(cache_db[resource['id']], resource['dateModified'])
        handler.fill_agreement.assert_called_with(munchify(resource['agreements'][0]), munchify(resource))
        handler.post_agreement.assert_called_with(munchify(resource['agreements'][0]))
        self.assertIn(resource['agreements'][0]['id'], cache_db)
        self.assertEquals(
            mocked_logger.info.call_args_list[3:],
            [
                call(
                    'Sync agreement {} of tender {}'.format(resource['agreements'][0]['id'], resource['id']),
                    extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': 'got_agreement_for_sync',
                           'JOURNAL_AGREEMENT_ID': resource['agreements'][0]['id']}
                )
            ]
        )
        # Resource gone
        resource['id'] = '9' * 32
        resource['agreements'] = [{'status': 'active', 'id': '10' * 16}]
        e = ResourceGone()
        handler.public_output_client.get_resource_item = MagicMock(side_effect=e)

        handler.process_resource(resource)

        self.assertIn(resource['id'], cache_db)
        self.assertEquals(cache_db[resource['id']], resource['dateModified'])
        self.assertEquals(
            mocked_logger.info.call_args_list[4:],
            [
                call(
                    'Sync agreement {} of tender {} has been archived'.format(resource['agreements'][0]['id'],
                                                                              resource['id']),
                    extra={'JOURNAL_TENDER_ID': resource['id'], 'MESSAGE_ID': 'skip_agreement',
                           'JOURNAL_AGREEMENT_ID': resource['agreements'][0]['id']}
                )
            ]
        )
