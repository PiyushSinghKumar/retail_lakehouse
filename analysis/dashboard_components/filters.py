"""Global filter component for the dashboard."""

import dash_bootstrap_components as dbc
from dash import dcc, html


def create_global_filter():
    """Create the dynamic filter bar that changes based on active tab."""
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("Year", style={"fontSize": "0.85rem", "marginBottom": "4px", "fontWeight": "500"}),
                                dcc.Dropdown(
                                    id="global-year",
                                    placeholder="All years",
                                    clearable=True,
                                    style={"minWidth": "120px"},
                                ),
                            ],
                            id="year-filter-col",
                            style={"marginRight": "12px"},
                        ),
                        html.Div(
                            [
                                html.Label("Month", style={"fontSize": "0.85rem", "marginBottom": "4px", "fontWeight": "500"}),
                                dcc.Dropdown(
                                    id="global-month",
                                    placeholder="Select month...",
                                    clearable=True,
                                    style={"minWidth": "140px"},
                                ),
                            ],
                            id="month-filter-col",
                            style={"display": "none", "marginRight": "12px"},
                        ),
                        html.Div(
                            [
                                html.Label("Week", style={"fontSize": "0.85rem", "marginBottom": "4px", "fontWeight": "500"}),
                                dcc.Dropdown(
                                    id="global-week",
                                    placeholder="Select week...",
                                    clearable=True,
                                    style={"minWidth": "120px"},
                                ),
                            ],
                            id="week-filter-col",
                            style={"display": "none"},
                        ),
                    ],
                    style={"display": "flex", "alignItems": "flex-start"},
                ),
            ],
            style={"padding": "12px"},
        ),
        style={
            "boxShadow": "0 2px 8px rgba(0, 0, 0, 0.15)",
            "border": "1px solid #dee2e6",
            "backgroundColor": "rgba(255, 255, 255, 0.98)",
        },
    )
