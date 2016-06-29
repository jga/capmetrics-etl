import configparser
import os
import re
import unittest
from click.testing import CliRunner
from capmetrics_etl import cli


class ParseConfigurationTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_config = os.path.join(tests_path, 'capmetrics.ini')

    def test_parse_configuration(self):
        config_parser = configparser.ConfigParser()
        # make parsing of config file names case-sensitive
        config_parser.optionxform = str
        config_parser.read(self.test_config)
        capmetrics_configuration = cli.parse_capmetrics_configuration(config_parser)
        self.assertEqual(capmetrics_configuration['engine_url'], 'sqlite:///:memory:')
        self.assertEqual(len(capmetrics_configuration['daily_ridership_worksheets']), 3)
        self.assertEqual(len(capmetrics_configuration['hour_productivity_worksheets']), 3)


class ETLCommandTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_config = os.path.join(tests_path, 'capmetrics_single.ini')

    def test_etl_command_line_test_flag(self):
        click_runner = CliRunner()
        arguments = ['./tests/data/test_cmta_data_single.xls', self.test_config, '--test']
        result = click_runner.invoke(cli.etl, arguments)
        self.assertEqual('Capmetrics Excel ETL test.', str(result.output).strip())

    def test_excel_etl_creation(self):
        click_runner = CliRunner()
        arguments = ['./tests/data/test_cmta_data_single.xls', self.test_config]
        result = click_runner.invoke(cli.etl, arguments)
        expected_message_start = r'Capmetrics Excel ETL starting...'
        message_regex = re.compile(expected_message_start)
        self.assertTrue(message_regex.match(str(result.output.strip())), msg=result)


class TablesCommandTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_config = os.path.join(tests_path, 'capmetrics_single.ini')

    def test_tables_command_line_test_flag(self):
        click_runner = CliRunner()
        arguments = [self.test_config, '--test']
        result = click_runner.invoke(cli.tables, arguments)
        self.assertEqual('Capmetrics table creation test.', str(result.output).strip())

    def test_table_creation(self):
        click_runner = CliRunner()
        arguments = [self.test_config]
        result = click_runner.invoke(cli.tables, arguments)
        self.assertEqual('Capmetrics database tables created.', result.output.strip())
