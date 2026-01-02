"""Dashboard layout components."""

import dash_bootstrap_components as dbc
from dash import dcc, html

from .filters import create_global_filter


def create_kpi_cards():
    """Create KPI summary cards."""
    return dbc.Row(
        [
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Total Sales", className="text-muted"),
                            html.H2(id="kpi-total-sales", className="text-success"),
                        ]
                    )
                ),
                width=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Total Transactions", className="text-muted"),
                            html.H2(id="kpi-total-transactions", className="text-info"),
                        ]
                    )
                ),
                width=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Avg Transaction", className="text-muted"),
                            html.H2(id="kpi-avg-transaction", className="text-primary"),
                        ]
                    )
                ),
                width=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Years of Data", className="text-muted"),
                            html.H2(id="kpi-years", className="text-warning"),
                        ]
                    )
                ),
                width=3,
            ),
        ],
        className="mb-4",
    )


def create_overview_tab():
    """Create the overview tab content."""
    return [
        dcc.Graph(id="overview-yearly-chart"),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="overview-region-chart"), width=6),
                dbc.Col(dcc.Graph(id="overview-category-chart"), width=6),
            ]
        ),
    ]


def create_yearly_tab():
    """Create the yearly drill-down tab content."""
    return [
        dcc.Graph(id="yearly-drill-chart"),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="yearly-region-chart"), width=6),
                dbc.Col(dcc.Graph(id="yearly-category-chart"), width=6),
            ]
        ),
    ]


def create_monthly_tab():
    """Create the monthly drill-down tab content (shows daily breakdown)."""
    return [
        dcc.Graph(id="monthly-drill-chart"),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="monthly-region-chart"), width=6),
                dbc.Col(dcc.Graph(id="monthly-category-chart"), width=6),
            ]
        ),
    ]


def create_weekly_tab():
    """Create the weekly drill-down tab content (shows daily breakdown)."""
    return [
        dcc.Graph(id="weekly-drill-chart"),
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="weekly-region-chart"), width=6),
                dbc.Col(dcc.Graph(id="weekly-category-chart"), width=6),
            ]
        ),
    ]


def create_layout():
    """Create the main dashboard layout."""
    return dbc.Container(
        [
            html.H1("Retail Business Dashboard", className="text-center my-4"),
            create_kpi_cards(),
            # Tabs container with filters inside
            html.Div(
                [
                    dbc.Tabs(
                        [
                            dbc.Tab(create_overview_tab(), label="Overview", tab_id="overview"),
                            dbc.Tab(create_yearly_tab(), label="Yearly Drill-Down", tab_id="yearly"),
                            dbc.Tab(create_monthly_tab(), label="Monthly Drill-Down", tab_id="monthly"),
                            dbc.Tab(create_weekly_tab(), label="Weekly Drill-Down", tab_id="weekly"),
                        ],
                        id="tabs",
                        active_tab="overview",
                        style={"justify-content": "space-evenly"},
                    ),
                    # Floating filter overlay - positioned at top right
                    html.Div(
                        create_global_filter(),
                        style={
                            "position": "absolute",
                            "top": "10px",
                            "right": "20px",
                            "zIndex": 1000,
                        },
                    ),
                ],
                style={"position": "relative", "marginBottom": "20px"},
            ),
        ],
        fluid=True,
    )
