import click
from . import etl


@click.command()
@click.option('--test', default=False)
def run(test):
    if not test:
        etl.run_excel_etl()
    else:
        click.echo('Capmetrics test.')

# Call run function when module deployed as script. This is approach is common
# within the python community
if __name__ == '__main__':
    run()
