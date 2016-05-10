import configparser
import os
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
        self.assertEqual(capmetrics_configuration['output'], '~/capmetrics-data/')
        self.assertEqual(capmetrics_configuration['source'], './tests/data/test_cmta_data.xls')
        self.assertEqual(len(capmetrics_configuration['daily_ridership_worksheets']), 3)
        self.assertEqual(len(capmetrics_configuration['hour_productivity_worksheets']), 3)


class ETLCommandTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_config = os.path.join(tests_path, 'capmetrics_single.ini')

    def test_etl_command_line_test_flag(self):
        click_runner = CliRunner()
        result = click_runner.invoke(cli.etl, [self.test_config, '--test'])
        self.assertEqual('Capmetrics Excel ETL test.', str(result.output).strip())

    def test_excel_etl_creation(self):
        click_runner = CliRunner()
        result = click_runner.invoke(cli.etl, [self.test_config])
        self.assertEqual('Capmetrics Excel ETL completed.', result.output.strip(), msg=result)


class TablesCommandTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_config = os.path.join(tests_path, 'capmetrics_single.ini')

    def test_tables_command_line_test_flag(self):
        click_runner = CliRunner()
        result = click_runner.invoke(cli.tables, [self.test_config, '--test'])
        self.assertEqual('Capmetrics table creation test.', str(result.output).strip())

    def test_table_creation(self):
        click_runner = CliRunner()
        result = click_runner.invoke(cli.tables, [self.test_config])
        self.assertEqual('Capmetrics database tables created.', result.output.strip())
