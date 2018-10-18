# -*- coding: utf-8 -*-
import unittest

from openprocurement.bridge.frameworkagreement.tests import filters, handlers, utils


def suite():
    tests = unittest.TestSuite()
    tests.addTest(handlers.suite())
    tests.addTest(filters.suite())
    tests.addTest(utils.suite())
    return tests


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
