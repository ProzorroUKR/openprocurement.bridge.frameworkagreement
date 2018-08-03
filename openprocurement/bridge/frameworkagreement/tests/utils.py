# -*- coding: utf-8 -*-
import unittest

from openprocurement.bridge.frameworkagreement.utils import journal_context, generate_req_id


class TestUtilsFucntions(unittest.TestCase):
    """Testing all functions inside utils.py."""

    def test_journal_context(self):
        self.assertEquals(journal_context(record={}, params={'test': 'test'}), {'JOURNAL_test': 'test'})

    def test_generate_req_id(self):
        req_id = generate_req_id()
        self.assertEquals(len(req_id), 64)
        self.assertEquals(req_id.startswith('contracting-data-bridge-req-'), True)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestUtilsFucntions))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
