import click
from dms_datastore.download_noaa import download_noaa_cli
from dms_datastore.download_hycom import download_hycom_cli
from dms_datastore.download_hrrr import download_hrrr_cli
from dms_datastore.download_cdec import download_cdec_cli
from dms_datastore.download_wdl import download_wdl_cli

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
cli.add_command(download_hrrr_cli, "download_hrrr")
cli.add_command(download_cdec_cli, "download_cdec")
cli.add_command(download_wdl_cli, "download_wdl")

if __name__ == "__main__":
    cli()
