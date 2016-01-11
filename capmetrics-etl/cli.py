import click
import etl


@click.command()
@click.argument('job', default='all')
@click.option('--test', default=False)
def run(job, test):
    if not test:
        if job == 'all':
            etl.update_routes()
            etl.update_modes()
    else:
        click.echo('Capmetrics test.')

# Call run function when module deployed as script. This is approach is common
# within the python community
if __name__ == '__main__':
    run()
