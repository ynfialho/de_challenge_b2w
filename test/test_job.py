from __future__ import absolute_import
import sys
import os
import json
sys.path.append('..')

import unittest
from de_challenge_b2w import process_abandoned_carts

OUTPUT_PATH = './output/abandoned-carts.json'
MOC_PATH = './test/files/moc.json'

class ProcessAbandonedCartsTest(unittest.TestCase):

    def setUp(self):
        process_abandoned_carts.run([])
    
    def tearDown(self):
        os.remove(OUTPUT_PATH)

    def test_output_run(self):
        output = json.load(open(OUTPUT_PATH))
        moc_validation = json.load(open(MOC_PATH))
        self.assertEquals(sorted(output.items()), sorted(moc_validation.items()))