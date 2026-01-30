import os

# Data directory - defaults to current directory, but can be set via DATA_DIR env var
# On Render, this will be /var/data (persistent disk mount point)
DATA_DIR = os.environ.get('DATA_DIR', '.')

def get_data_path(relative_path: str) -> str:
    """Get the full path for a data file, relative to DATA_DIR."""
    return os.path.join(DATA_DIR, relative_path)
