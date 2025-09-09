
import typer, polars as pl
from .core import AISDataset

app = typer.Typer(help="AIS Toolkit CLI")

@app.command()
def scan(root: str,
         pattern: str = "*.csv",
         date_from: str = typer.Option(None, "--from"),
         date_to: str = typer.Option(None, "--to"),
         mmsi: str = typer.Option(None, help="Single MMSI or comma-separated list"),
         cols: str = typer.Option(None, help="Comma-separated columns"),
         to_parquet: str = typer.Option(None, help="Output Parquet path"),
         html: str = typer.Option(None, help="Save track map to HTML")):
    ds = AISDataset(root, pattern=pattern)
    if cols:
        ds = ds.with_columns([c.strip() for c in cols.split(",") if c.strip()])
    if date_from and date_to:
        ds = ds.between(date_from, date_to)
    if mmsi:
        values = [int(x.strip()) for x in mmsi.split(",")]
        ds = ds.filter(mmsi=values)
    if to_parquet:
        ds.write_parquet(to_parquet)
        typer.echo(f"Written Parquet to {to_parquet}")
    if html:
        ds.plot_map(html)
        typer.echo(f"Wrote map to {html}")

if __name__ == "__main__":
    app()
