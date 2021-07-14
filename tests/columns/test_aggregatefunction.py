from enum import IntEnum

from tests.testcase import BaseTestCase


class AggregateFunctionTestCase(BaseTestCase):
    required_server_version = (19, 8, 3)

    def test_simple_sum_agg_function(self):
        columns = 'a AggregateFunction(sum, Int32)'

        data = [("arrayReduce('sumState', [1,2,3])", ), ("arrayReduce('sumState', [3,8])", )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT sumMerge(a) FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '17\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

        # TODO: uniqExact, anyIf, 
