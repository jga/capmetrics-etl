import click
import configparser

import json
from . import etl




def parse_capmetrics_configuration(config_parser):
    worksheet_names = json.loads(config_parser['capmetrics']['worksheet_names'])
    capmetrics_configuration = {
        'timezone': 'America/Chicago',
        'engine': config_parser['capmetrics']['engine'],
        'worksheet_names': worksheet_names
    }
    if 'timezone' in config_parser['capmetrics']:
        capmetrics_configuration['timezone'] = config_parser['capmetrics']['timezone']
    return capmetrics_configuration

@click.command()
@click.argument('config')
@click.option('--test', default=False)
def run(config, test):
    if not test:
        config_parser = configparser.ConfigParser()
        # make parsing of config file names case-sensitive
        config_parser.optionxform = str
        config_parser.read(config)
        capmetrics_configuration = parse_capmetrics_configuration(config_parser)
        etl.run_excel_etl(capmetrics_configuration)
    else:
        click.echo('Capmetrics CLI test.')

# Call run function when module deployed as script. This is approach is common
# within the python community
if __name__ == '__main__':
    run()
