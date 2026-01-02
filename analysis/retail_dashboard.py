"""
Interactive retail business dashboard with global filtering and drill-down analysis.

Usage:
    python retail_dashboard.py

Access at: http://localhost:8050
"""

import json
from pathlib import Path

import dash_bootstrap_components as dbc
from dash import Dash

# Import dashboard components
from dashboard_components.layout import create_layout
from dashboard_components.callbacks import register_callbacks

# Load configuration
with open(Path(__file__).parent.parent / "config.json") as f:
    config = json.load(f)

AGGREGATED_DIR = Path(config["aggregated_dir"])

# Initialize the Dash app
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="Retail Dashboard"
)

# Set the layout
app.layout = create_layout()

# Register all callbacks
register_callbacks(app, AGGREGATED_DIR)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
