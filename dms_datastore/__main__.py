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
from dms_datastore.download_dcc import download_dcc_cli
from dms_datastore.download_montezuma_gates import download_montezuma_gates_cli
from dms_datastore.download_smscg import download_smscg_cli
from dms_datastore.compare_directories import compare_dir_cli
from dms_datastore.populate_repo import populate_main_cli
from dms_datastore.station_info import station_info_cli
from dms_datastore.reformat import reformat_cli

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
cli.add_command(download_dcc_cli, "download_dcc")
cli.add_command(download_montezuma_gates_cli, "download_montezuma_gates")
cli.add_command(download_smscg_cli, "download_smscg")
cli.add_command(compare_dir_cli, "compare_directories")
cli.add_command(populate_main_cli, "populate_repo")
cli.add_command(station_info_cli, "station_info")
cli.add_command(reformat_cli, "reformat")

if __name__ == "__main__":
    cli()
