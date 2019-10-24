import copy
from unittest.mock import patch, MagicMock

from transform_data import run


class TestTransformation:

    @patch('transform_data.psycopg2')
    def test_all_transformations(self, db_driver):
        scenario = self.Scenario(db_driver)

        scenario.given_a_row_that_is_retrieved_from_the_db()
        scenario.given_the_row_contains_a_country_code_for_uk()

        scenario.when_running_the_transformation()

        scenario.then_the_data_will_insert_the_country()

    class Scenario:

        TEST_ID = 1
        TEST_COUNTRY_CODE = 'code'
        TEST_COMPANY_ID = 3

        def __init__(self, db_driver):
            self.db_driver = db_driver

            self.connection = MagicMock()
            self.db_driver.connect.return_value = self.connection

            self.cursor_reader = MagicMock()
            self.cursor_writer = MagicMock()
            self.connection.cursor.side_effect = [self.cursor_reader, self.cursor_writer]

            self.rows = []
            self.cursor_reader.fetchall.return_value = self.rows

            self.default_row_data = [self.TEST_ID, self.TEST_COUNTRY_CODE, self.TEST_COMPANY_ID ]

        def given_a_row_that_is_retrieved_from_the_db(self):
            self.retrieved_row = copy.deepcopy(self.default_row_data)
            self.rows.append(self.retrieved_row)

        def given_the_row_contains_a_country_code_for_uk(self):
            self.retrieved_row[1] = 'uk'

        def when_running_the_transformation(self):
            run()
            self.execute_call_args = self.cursor_writer.execute.call_args[0]
            self.insert_ags = self.execute_call_args[1]

        def then_the_data_will_insert_the_country(self):
            assert self.insert_ags[1] == 'uk'


