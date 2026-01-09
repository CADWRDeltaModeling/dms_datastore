import click
from dms_datastore.download_noaa import download_noaa_cli
from dms_datastore.download_hycom import download_hycom_cli

@click.group(
    help="DMS CLI tools for data processing and extraction."
)
@click.help_option("-h", "--help")  # Add the help option at the group level
def cli():
    """Main entry point for dms_datastore commands."""
    pass

# Register the commands
cli.add_command(download_noaa_cli, "download_noaa")
cli.add_command(download_hycom_cli, "download_hycom")

if __name__ == "__main__":
    cli()
