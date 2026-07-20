"""Enable ``python -m labdata`` (used by the download DAG to shell back in)."""

from labdata.cli import app

if __name__ == "__main__":
    app()
