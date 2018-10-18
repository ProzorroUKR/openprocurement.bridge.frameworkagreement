import unittest
from mock import MagicMock, patch, call
from gevent.queue import PriorityQueue

from openprocurement.bridge.frameworkagreement.filters import CFAUAFilter


CONFIG = {
    'filter_config': {
        'statuses': [],
        'procurementMethodTypes': [],
        'lot_status': None,
        'timeout': 0,
        'filters': [],
    },
    'resource': 'tenders'
}


class TestResourceFilters(unittest.TestCase):
    db = {}
    conf = CONFIG

    @patch('openprocurement.bridge.frameworkagreement.filters.INFINITY')
    @patch('openprocurement.bridge.frameworkagreement.filters.logger')
    def test_CFAUAFilter(self, logger, infinity):
        self.input_queue = PriorityQueue()
        self.filtered_queue = PriorityQueue()
    
        resource = self.conf['resource'][:-1]

        filter = CFAUAFilter(self.conf, self.input_queue, self.filtered_queue, self.db)
        mock_calls = [call.info('Init Close Framework Agreement Filter.')]
        self.assertEqual(logger.mock_calls, mock_calls)
        extra = {'MESSAGE_ID': 'SKIPPED', 'JOURNAL_{}_ID'.format(resource.upper()): 'test_id'}

        infinity.__nonzero__.side_effect = [True, False]
        filter._run()

        doc = {
            'id': 'test_id',
            'dateModified': '1970-01-01'
        }

        self.input_queue.put((None, doc))
        self.db['test_id'] = '1970-01-01'
        infinity.__nonzero__.side_effect = [True, False]
        filter._run()
        mock_calls.append(
            call.info('{} test_id not modified from last check. Skipping'.format(resource.title()),
            extra=extra)
        )
        self.assertEqual(logger.mock_calls, mock_calls)

        doc['procurementMethodType'] = 'test'
        doc['dateModified'] = '1970-01-02'
        self.input_queue.put((None, doc))
        infinity.__nonzero__.side_effect = [True, False]
        filter._run()
        mock_calls.append(
            call.info('Skipping test {} test_id'.format(resource),
            extra=extra)
        )
        self.assertEqual(logger.mock_calls, mock_calls)

        filter.procurement_method_types = ('test')
        doc['status'] = 'test_status'
        self.input_queue.put((None, doc))
        infinity.__nonzero__.side_effect = [True, False]
        filter._run()
        mock_calls.append(
            call.info('Skipping test {} test_status test_id'.format(resource),
            extra=extra)
        )
        self.assertEqual(logger.mock_calls, mock_calls)

        filter.statuses = ('test_status')
        filter.lot_status = 'test_status'
        doc['lots'] = [{'status': 'spam_status'}]
        self.input_queue.put((None, doc))
        infinity.__nonzero__.side_effect = [True, False]
        filter._run()
        mock_calls.append(
            call.info('Skipping multilot {} test_id in status test_status'.format(resource),
            extra=extra)
        )
        self.assertEqual(logger.mock_calls, mock_calls)

        del doc['lots']
        self.input_queue.put((None, doc))
        infinity.__nonzero__.side_effect = [True, False]
        filter._run()
        mock_calls.append(
            call.debug('Put to filtered queue {} test_id'.format(resource))
        )
        self.assertEqual(logger.mock_calls, mock_calls)
        priority, resource = self.filtered_queue.get()
        self.assertEqual(priority, None)
        self.assertEqual(resource, doc)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestResourceFilters))
    return suite


if __name__ == "__main__":
    unittest.main(defaultTest='suite')
