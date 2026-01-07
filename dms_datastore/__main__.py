import click

@click.group(
    help="DMS CLI tools for data processing and extraction."
)
@click.help_option("-h", "--help")  # Add the help option at the group level
def cli():
    """Main entry point for dms_datastore commands."""
    pass

if __name__ == "__main__":
    cli()
