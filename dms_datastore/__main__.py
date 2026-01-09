import click
from dms_datastore.download_noaa import download_noaa_cli
from dms_datastore.download_hycom import download_hycom_cli
from dms_datastore.download_hrrr import download_hrrr_cli
from dms_datastore.download_cdec import download_cdec_cli
from dms_datastore.download_wdl import download_wdl_cli
from dms_datastore.download_nwis import download_nwis_cli
from dms_datastore.download_des import download_des_cli
from dms_datastore.download_ncro_cnra import download_ncro_cnra_cli
from dms_datastore.download_mokelumne import download_mokelumne_cli
from dms_datastore.download_ucdipm import download_ucdipm_cli
from dms_datastore.download_cimis import download_cimis_cli

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
cli.add_command(download_nwis_cli, "download_nwis")
cli.add_command(download_des_cli, "download_des")
cli.add_command(download_ncro_cnra_cli, "download_ncro")
cli.add_command(download_mokelumne_cli, "download_mokelumne")
cli.add_command(download_ucdipm_cli, "download_ucdipm")
cli.add_command(download_cimis_cli, "download_cimis")

if __name__ == "__main__":
    cli()
