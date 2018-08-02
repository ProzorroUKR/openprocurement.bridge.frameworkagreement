# -*- coding: utf-8 -*-
import unittest

from openprocurement.bridge.frameworkagreement.utils import journal_context


class TestUtilsFucntions(unittest.TestCase):
    """Testing all functions inside utils.py."""

    def test_journal_context(self):
        self.assertEquals(journal_context(record={}, params={'test': 'test'}), {'JOURNAL_test': 'test'})


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestUtilsFucntions))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')