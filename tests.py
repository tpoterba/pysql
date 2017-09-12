from __future__ import print_function  # Python 2 and 3 print compatibility
import unittest
from ast import *


class Tests(unittest.TestCase):
    def test1(self):
        load = Load('resources/table1.tsv', {'A': Type.Int, 'B': Type.String, 'C': Type.Int, 'D': Type.String})
        self.assertEqual(Count(load).execute(), 3)

        filtered = Filter(load, Equal(Column('B'), Column('D')))
        self.assertEqual(Count(filtered).execute(), 1)

        counter = Counter(load, Column('B')).execute()
        self.assertEqual(counter, {'Foo': 2, 'Baz': 1})

        counter2 = Counter(load, Column('C')).execute()
        self.assertEqual(counter2, {1: 1, 4: 1, None: 1})

        Write(load, '/tmp/out1.tsv').execute()

        self.assertEqual([x.strip().split() for x in open('resources/table1.tsv', 'r')],
                         [x.strip().split() for x in open('/tmp/out1.tsv', 'r')])

    def test_select(self):
        load = Load('resources/table1.tsv', {'A': Type.Int, 'B': Type.String, 'C': Type.Int, 'D': Type.String})

        s = Select(load, [('Foo', Column('A')), ('Bar', LessThan(Column('C'), Column('A')))])
        rows = Collect(s).execute()

        self.assertEqual(rows, [{'Foo': 1, 'Bar': None}, {'Foo': 2, 'Bar': False}, {'Foo': 2, 'Bar': True}])
