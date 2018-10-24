import unittest

from datetime import datetime
from mock import patch, MagicMock, call
from munch import munchify

from openprocurement_client.exceptions import (
    ResourceNotFound,
    ResourceGone
)

from openprocurement.bridge.frameworkagreement.tests.base import AdaptiveCache
from openprocurement.bridge.frameworkagreement.handlers import (
    AgreementObjectMaker, CFASelectionUAHandler
)


class TestAgreementObjectMaker(unittest.TestCase):
    config = {'worker_config': {'handler_cfaua': {
        'input_resources_api_token': 'resources_api_token',
        'output_resources_api_token': 'resources_api_token',
        'resources_api_version': 'resources_api_version',
        'input_resources_api_server': 'resources_api_server',
        'input_public_resources_api_server': 'public_resources_api_server',
        'input_resource': 'resource',
        'output_resources_api_server': 'resources_api_server',
        'output_public_resources_api_server': 'public_resources_api_server',
        'output_resource': 'output_resource'
    }}}

    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_init(self, mocked_logger, mocked_client):
        handler = AgreementObjectMaker(self.config, 'cache_db')

        self.assertEquals(handler.cache_db, 'cache_db')
        self.assertEquals(handler.handler_config, self.config['worker_config']['handler_cfaua'])
        self.assertEquals(handler.main_config, self.config)
        self.assertEquals(handler.config_keys,
                          ('input_resources_api_token', 'output_resources_api_token', 'resources_api_version',
                           'input_resources_api_server',
                           'input_public_resources_api_server', 'input_resource', 'output_resources_api_server',
                           'output_public_resources_api_server', 'output_resource')
                          )
        self.assertEquals(handler.keys_from_tender, ('procuringEntity',))
        mocked_logger.info.assert_called_once_with('Init Close Framework Agreement UA Handler.')

    @patch('openprocurement.bridge.basic.handlers.APIClient')
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

    @patch('openprocurement.bridge.basic.handlers.APIClient')
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

    @patch('openprocurement.bridge.basic.handlers.APIClient')
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
        self.assertTrue(cache_db.has(resource['id']))

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
        self.assertTrue(cache_db.has(resource['id']))

        # not in local db
        resource['id'] = '5' * 32
        resource['agreements'] = [{'status': 'active', 'id': '6' * 32}]

        handler.process_resource(resource)

        handler.public_output_client.get_resource_item.assert_called_with(resource['agreements'][0]['id'])
        self.assertTrue(cache_db.has(resource['agreements'][0]['id']))
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

        self.assertTrue(cache_db.has(resource['id']))
        self.assertEquals(cache_db[resource['id']], resource['dateModified'])
        handler.fill_agreement.assert_called_with(munchify(resource['agreements'][0]), munchify(resource))
        handler.post_agreement.assert_called_with(munchify(resource['agreements'][0]))
        self.assertTrue(cache_db.has(resource['agreements'][0]['id']))
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

        self.assertTrue(cache_db.has(resource['id']))
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


class TestCFASelectionUAHandler(unittest.TestCase):
    config = {'worker_config': {'handler_cfaselectionua': {
        'input_resources_api_token': 'resources_api_token',
        'output_resources_api_token': 'resources_api_token',
        'resources_api_version': 'resources_api_version',
        'input_resources_api_server': 'resources_api_server',
        'input_public_resources_api_server': 'public_resources_api_server',
        'input_resource': 'resource',
        'output_resources_api_server': 'resources_api_server',
        'output_public_resources_api_server': 'public_resources_api_server',
        'output_resource': 'output_resource'
    }}}

    @patch('openprocurement.bridge.frameworkagreement.handlers.coordination')
    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_init(self, mocked_logger, mocked_client, mocked_coordination):
        coordinator = MagicMock()
        mocked_coordination.get_coordinator.return_value = coordinator
        handler = CFASelectionUAHandler(self.config, 'cache_db')

        self.assertEquals(handler.cache_db, 'cache_db')
        self.assertEquals(handler.handler_config, self.config['worker_config']['handler_cfaselectionua'])
        self.assertEquals(handler.main_config, self.config)
        self.assertEqual(handler.coordinator, coordinator)
        self.assertEquals(handler.config_keys,
                          ('input_resources_api_token', 'output_resources_api_token', 'resources_api_version',
                           'input_resources_api_server',
                           'input_public_resources_api_server', 'input_resource', 'output_resources_api_server',
                           'output_public_resources_api_server', 'output_resource')
                          )
        mocked_logger.info.assert_called_once_with('init CFA Selection UA Handler.')
        mocked_coordination.get_coordinator.assert_called_once_with('redis://', 'bridge')
        coordinator.start.assert_called_once_with(start_heart=True)

    @patch('openprocurement.bridge.frameworkagreement.handlers.coordination')
    @patch('openprocurement.bridge.basic.handlers.APIClient')
    @patch('openprocurement.bridge.frameworkagreement.handlers.logger')
    def test_process_resource(self, mocked_logger, mocked_client, mocked_coordination):
        lock = MagicMock()

        coordinator = MagicMock()
        coordinator.get_lock.return_value = lock

        mocked_coordination.get_coordinator.return_value = coordinator
        cache_db = AdaptiveCache({'0' * 32: datetime.now()})
        handler = CFASelectionUAHandler(self.config, cache_db)

        # test lock _client exists
        resource = {'id': '0' * 32}
        handler.process_resource(resource)
        self.assertEquals(
            mocked_logger.info.call_args_list[1:],
            [
                call(
                    'Tender {} processing by another worker.'.format(resource['id']),
                    extra={'JOURNAL_TENDER_ID': resource['id'],
                           'MESSAGE_ID': 'tender_already_in_process'}
                )
            ]
        )

        # test actual process agreements
        agreement = {'data': {'id': '1' * 32, 'status': 'active'}}
        resource['agreements'] = [{'id': '1' * 32}]
        resource['status'] = 'active.enquires'
        lock._client.exists.return_value = False
        handler.input_client.get_resource_item.return_value = agreement
        handler.output_client.patch_resource_item.return_value = {
            'data': {'status': resource['status']}
        }

        handler.process_resource(resource)
        self.assertEquals(
            mocked_logger.info.call_args_list[2:],
            [
                call(
                    'Received agreement data {}'.format(resource['agreements'][0]['id']),
                    extra={'JOURNAL_TENDER_ID': resource['id'],
                           'MESSAGE_ID': 'received_agreement_data',
                           'JOURNAL_AGREEMENT_ID': resource['agreements'][0]['id']}
                ),
                call(
                    'Patch tender agreement {}'.format(resource['agreements'][0]['id']),
                    extra={'JOURNAL_TENDER_ID': resource['id'],
                           'MESSAGE_ID': 'patch_agreement_data',
                           'JOURNAL_AGREEMENT_ID': resource['agreements'][0]['id']}
                ),
                call(
                    'Switch tender {} status to {}'.format(resource['id'], resource['status']),
                    extra={'JOURNAL_TENDER_ID': resource['id'],
                           'MESSAGE_ID': 'patch_tender_status'}
                ),
            ]
        )
        handler.input_client.get_resource_item.assert_called_with(resource['agreements'][0]['id'])

        handler.output_client.patch_resource_item_subitem.assert_called_with(
            resource['id'], agreement, 'agreements', subitem_id=resource['agreements'][0]['id']
        )
        handler.output_client.patch_resource_item(
            resource['id'], {'data': {'status': 'active.enquiries'}}
        )

        # test invalid/doesnt exist agreement
        resource = {'id': '1' * 32}
        resource['agreements'] = [{'id': '1' * 32}]
        resource['status'] = 'draft.unsuccessful'
        lock._client.exists.return_value = False
        e = ResourceNotFound()
        handler.input_client.get_resource_item = MagicMock()
        handler.input_client.get_resource_item.side_effect = (e, e, e, resource)
        handler.output_client.patch_resource_item.return_value = {
            'data': {'status': resource['status']}
        }

        handler.process_resource(resource)
        self.assertEquals(
            mocked_logger.info.call_args_list[5:],
            [
                call(
                    'Switch tender {} status to {}'.format(resource['id'], 'draft.unsuccessful'),
                    extra={'JOURNAL_TENDER_ID': resource['id'],
                           'MESSAGE_ID': 'patch_tender_status'}
                )
            ]
        )
        handler.output_client.patch_resource_item.called_with(resource['id'],
                                                              {'data': {'status': 'draft.unsuccessful'}})
        handler.input_client.get_resource_item.called_with(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
        self.assertEqual(len(handler.input_client.get_resource_item.call_args_list), 3)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestAgreementObjectMaker))
    suite.addTest(unittest.makeSuite(TestCFASelectionUAHandler))
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest='suite')
