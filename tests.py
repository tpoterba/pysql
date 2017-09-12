from __future__ import print_function  # Python 2 and 3 print compatibility
import unittest
from ast import *


class Tests(unittest.TestCase):
    def test1(self):
        load = Load('resources/table1.tsv', Schema({'A': IntType, 'B': StringType, 'C': IntType, 'D': StringType}))
        self.assertEqual(Count(load).compute(), 2)
        filter = Filter(load, Equal(Column('B'), Column('D')))
        self.assertEqual(Count(filter).compute(), 1)
