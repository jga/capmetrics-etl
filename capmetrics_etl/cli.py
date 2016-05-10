import click
import configparser

import json
from . import etl


def parse_capmetrics_configuration(config_parser):
    source = config_parser['capmetrics']['source']
    daily_worksheets = json.loads(config_parser['capmetrics']['daily_ridership_worksheets'])
    hourly_worksheets = json.loads(config_parser['capmetrics']['hourly_ridership_worksheets'])
    capmetrics_configuration = {
        'source': source,
        'timezone': 'America/Chicago',
        'engine': config_parser['capmetrics']['engine'],
        'daily_ridership_worksheets': daily_worksheets,
        'hourly_ridership_worksheets': hourly_worksheets
    }
    if 'timezone' in config_parser['capmetrics']:
        capmetrics_configuration['timezone'] = config_parser['capmetrics']['timezone']
    return capmetrics_configuration


@click.command()
@click.argument('config')
@click.option('--test', default=False)
def etl(config, test):
    if not test:
        config_parser = configparser.ConfigParser()
        # make parsing of config file names case-sensitive
        config_parser.optionxform = str
        config_parser.read(config)
        capmetrics_configuration = parse_capmetrics_configuration(config_parser)
        report, timestamp = etl.run_excel_etl(capmetrics_configuration)
        etl.write_etl_report(report, timestamp)
    else:
        click.echo('Capmetrics CLI test.')


@click.command()
@click.argument('config')
@click.option('--test', default=False)
def tables(config, test):
    if not test:
        config_parser = configparser.ConfigParser()
        # make parsing of config file names case-sensitive
        config_parser.optionxform = str
        config_parser.read(config)
        capmetrics_configuration = parse_capmetrics_configuration(config_parser)
        engine = create_engine(capmetrics_configuration['engine_url'])
        create_tables(engine)
        click.echo('Capmetrics database tables created.')
    else:
        click.echo('Capmetrics table creation test.')

