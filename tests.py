from __future__ import print_function  # Python 2 and 3 print compatibility
import unittest
from ast import *


class Tests(unittest.TestCase):
    def test1(self):
        load = Load('resources/table1.tsv', {'A': IntType, 'B': StringType, 'C': IntType, 'D': StringType})
        self.assertEqual(Count(load).compute(), 3)

        filtered = Filter(load, Equal(Column('B'), Column('D')))
        self.assertEqual(Count(filtered).compute(), 1)

        counter = Counter(load, Column('B')).compute()
        self.assertEqual(counter, {'Foo': 2, 'Baz': 1})

        counter2 = Counter(load, Column('C')).compute()
        self.assertEqual(counter2, {1: 1, 4: 1, None: 1})

        Write(load, '/tmp/out1.tsv').compute()

        self.assertEqual([x.strip().split() for x in open('resources/table1.tsv', 'r')],
                         [x.strip().split() for x in open('/tmp/out1.tsv', 'r')])
