import click
import configparser
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from capmetrics_etl.etl import create_tables, run_excel_etl, update_perfdocs
from capmetrics_etl.quality import check_quality


def parse_capmetrics_configuration(config_parser):
    daily_worksheets = json.loads(config_parser['capmetrics']['daily_ridership_worksheets'])
    hourly_worksheets = json.loads(config_parser['capmetrics']['hour_productivity_worksheets'])
    capmetrics_configuration = {
        'engine_url': config_parser['capmetrics']['engine_url'],
        'daily_ridership_worksheets': daily_worksheets,
        'hour_productivity_worksheets': hourly_worksheets
    }
    return capmetrics_configuration


@click.command()
@click.argument('file')
@click.argument('config')
@click.option('--perfdocs', is_flag=True)
@click.option('--test', is_flag=True)
def etl(file, config, perfdocs, test):
    if not test:
        if not perfdocs:
            click.echo('Capmetrics Excel ETL starting...')
            config_parser = configparser.ConfigParser()
            # make parsing of config file names case-sensitive
            config_parser.optionxform = str
            config_parser.read(config)
            capmetrics_configuration = parse_capmetrics_configuration(config_parser)
            # run data quality 'sanity check' before getting all dressed up to talk to db
            daily_worksheets = capmetrics_configuration['daily_ridership_worksheets']
            hour_worksheets = capmetrics_configuration['hour_productivity_worksheets']
            worksheet_names = daily_worksheets + hour_worksheets
            if check_quality(file, worksheet_names):
                connection_configuration = {"options": "-c timezone=utc"}
                engine = create_engine(capmetrics_configuration['engine_url'],
                                       connect_args=connection_configuration)
                has_table = engine.dialect.has_table(engine.connect(), 'route')
                if not has_table:
                    create_tables(engine)
                Session = sessionmaker()
                Session.configure(bind=engine)
                session = Session()
                run_excel_etl(file, session, capmetrics_configuration)
                click.echo('Capmetrics Excel ETL completed.')
            else:
                click.echo('Capmetrics stopped ETL. Source file data is incorrectly formatted.')
        else:
            click.echo('Capmetrics performance document update starting...')
            config_parser = configparser.ConfigParser()
            # make parsing of config file names case-sensitive
            config_parser.optionxform = str
            config_parser.read(config)
            capmetrics_configuration = parse_capmetrics_configuration(config_parser)
            # run data quality 'sanity check' before getting all dressed up to talk to db
            daily_worksheets = capmetrics_configuration['daily_ridership_worksheets']
            hour_worksheets = capmetrics_configuration['hour_productivity_worksheets']
            worksheet_names = daily_worksheets + hour_worksheets
            if check_quality(file, worksheet_names):
                connection_configuration = {"options": "-c timezone=utc"}
                engine = create_engine(capmetrics_configuration['engine_url'],
                                       connect_args=connection_configuration)
                has_table = engine.dialect.has_table(engine.connect(), 'route')
                if not has_table:
                    create_tables(engine)
                Session = sessionmaker()
                Session.configure(bind=engine)
                session = Session()
                update_perfdocs(session)
                click.echo('Capmetrics performance document update completed.')
            else:
                click.echo('Capmetrics stopped ETL. Source file data is incorrectly formatted.')

    else:
        click.echo('Capmetrics Excel ETL test.')


@click.command()
@click.argument('config')
@click.option('--test', is_flag=True)
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

