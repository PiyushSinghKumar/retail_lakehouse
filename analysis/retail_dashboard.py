"""
Interactive retail business dashboard with drill-down time-series analysis.

Usage:
    python retail_dashboard.py

Access at: http://localhost:8050
"""

import json
from pathlib import Path

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import polars as pl
from dash import Dash, Input, Output, dcc, html

with open(Path(__file__).parent.parent / "config.json") as f:
    config = json.load(f)

AGGREGATED_DIR = Path(config["aggregated_dir"])

app = Dash(
    __name__, external_stylesheets=[dbc.themes.BOOTSTRAP], title="Retail Dashboard"
)


def load_yearly_sales():
    file_path = AGGREGATED_DIR / "sales_yearly.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_yearly_by_region():
    file_path = AGGREGATED_DIR / "sales_yearly_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_yearly_by_category():
    file_path = AGGREGATED_DIR / "sales_yearly_by_category.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_monthly_sales():
    file_path = AGGREGATED_DIR / "sales_monthly.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_monthly_by_region():
    file_path = AGGREGATED_DIR / "sales_monthly_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_monthly_by_category():
    file_path = AGGREGATED_DIR / "sales_monthly_by_category.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_weekly_sales():
    file_path = AGGREGATED_DIR / "sales_weekly.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_weekly_by_region():
    file_path = AGGREGATED_DIR / "sales_weekly_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_weekly_by_category():
    file_path = AGGREGATED_DIR / "sales_weekly_by_category.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_daily_sales():
    file_path = AGGREGATED_DIR / "sales_daily.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_daily_by_region():
    file_path = AGGREGATED_DIR / "sales_daily_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_daily_by_category():
    file_path = AGGREGATED_DIR / "sales_daily_by_category.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_sales_by_region():
    file_path = AGGREGATED_DIR / "sales_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_top_categories():
    file_path = AGGREGATED_DIR / "top_categories.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def create_yearly_chart():
    df = load_yearly_sales()

    if df.is_empty():
        return go.Figure()

    df_pd = df.to_pandas()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df_pd["year"],
            y=df_pd["total_sales"],
            mode="lines+markers",
            name="Total Sales",
            line=dict(color="royalblue", width=2),
            marker=dict(size=8),
        )
    )

    fig.update_layout(
        title="Yearly Sales Overview",
        xaxis_title="Year",
        yaxis_title="Total Sales (€)",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_yearly_region_chart(selected_year=None):
    df = load_yearly_by_region()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("year") == selected_year)

    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["region"],
                y=df_pd["total_sales"],
                marker_color="teal",
            )
        ]
    )

    title = f"Sales by Region - {selected_year}" if selected_year else "Sales by Region"

    fig.update_layout(
        title=title,
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_yearly_category_chart(selected_year=None):
    df = load_yearly_by_category()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("year") == selected_year)

    df = df.sort("total_sales", descending=True).head(10)
    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["category"],
                y=df_pd["total_sales"],
                marker_color="purple",
            )
        ]
    )

    title = f"Top 10 Categories - {selected_year}" if selected_year else "Top 10 Categories"

    fig.update_layout(
        title=title,
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_monthly_chart(selected_year=None):
    df = load_monthly_sales()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("year") == selected_year)

    df_pd = df.to_pandas()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df_pd["year_month"],
            y=df_pd["total_sales"],
            mode="lines+markers",
            name="Total Sales",
            line=dict(color="royalblue", width=2),
            marker=dict(size=6),
        )
    )

    title = f"Monthly Sales - {selected_year}" if selected_year else "Monthly Sales"

    fig.update_layout(
        title=title,
        xaxis_title="Month",
        yaxis_title="Total Sales (€)",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_monthly_region_chart(selected_year=None):
    df = load_monthly_by_region()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("year") == selected_year)

    df = df.group_by("region").agg(pl.sum("total_sales")).sort("total_sales", descending=True)
    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["region"],
                y=df_pd["total_sales"],
                marker_color="teal",
            )
        ]
    )

    title = f"Sales by Region - {selected_year}" if selected_year else "Sales by Region"

    fig.update_layout(
        title=title,
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_monthly_category_chart(selected_year=None):
    df = load_monthly_by_category()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("year") == selected_year)

    df = df.group_by("category").agg(pl.sum("total_sales")).sort("total_sales", descending=True).head(10)
    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["category"],
                y=df_pd["total_sales"],
                marker_color="purple",
            )
        ]
    )

    title = f"Top 10 Categories - {selected_year}" if selected_year else "Top 10 Categories"

    fig.update_layout(
        title=title,
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_weekly_chart(selected_year=None):
    df = load_weekly_sales()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("iso_year") == selected_year)

    df_pd = df.to_pandas()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df_pd["iso_week"],
            y=df_pd["total_sales"],
            mode="lines+markers",
            name="Total Sales",
            line=dict(color="green", width=2),
            marker=dict(size=4),
        )
    )

    title = f"Weekly Sales - {selected_year}" if selected_year else "Weekly Sales"

    fig.update_layout(
        title=title,
        xaxis_title="Week Number",
        yaxis_title="Total Sales (€)",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_weekly_region_chart(selected_year=None):
    df = load_weekly_by_region()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("iso_year") == selected_year)

    df = df.group_by("region").agg(pl.sum("total_sales")).sort("total_sales", descending=True)
    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["region"],
                y=df_pd["total_sales"],
                marker_color="teal",
            )
        ]
    )

    title = f"Sales by Region - {selected_year}" if selected_year else "Sales by Region"

    fig.update_layout(
        title=title,
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_weekly_category_chart(selected_year=None):
    df = load_weekly_by_category()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("iso_year") == selected_year)

    df = df.group_by("category").agg(pl.sum("total_sales")).sort("total_sales", descending=True).head(10)
    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["category"],
                y=df_pd["total_sales"],
                marker_color="purple",
            )
        ]
    )

    title = f"Top 10 Categories - {selected_year}" if selected_year else "Top 10 Categories"

    fig.update_layout(
        title=title,
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_daily_chart(selected_year=None, selected_month=None):
    df = load_daily_sales()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("year") == selected_year)
    if selected_month:
        df = df.filter(pl.col("month") == selected_month)

    df_pd = df.to_pandas()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df_pd["date"],
            y=df_pd["total_sales"],
            mode="lines+markers",
            name="Total Sales",
            line=dict(color="orange", width=2),
            marker=dict(size=4),
        )
    )

    title = "Daily Sales"
    if selected_year and selected_month:
        title = f"Daily Sales - {selected_year}-{selected_month:02d}"
    elif selected_year:
        title = f"Daily Sales - {selected_year}"

    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Total Sales (€)",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_daily_region_chart(selected_year=None, selected_month=None):
    df = load_daily_by_region()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("year") == selected_year)
    if selected_month:
        df = df.filter(pl.col("month") == selected_month)

    df = df.group_by("region").agg(pl.sum("total_sales")).sort("total_sales", descending=True)
    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["region"],
                y=df_pd["total_sales"],
                marker_color="teal",
            )
        ]
    )

    title = "Sales by Region"
    if selected_year and selected_month:
        title = f"Sales by Region - {selected_year}-{selected_month:02d}"
    elif selected_year:
        title = f"Sales by Region - {selected_year}"

    fig.update_layout(
        title=title,
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_daily_category_chart(selected_year=None, selected_month=None):
    df = load_daily_by_category()

    if df.is_empty():
        return go.Figure()

    if selected_year:
        df = df.filter(pl.col("year") == selected_year)
    if selected_month:
        df = df.filter(pl.col("month") == selected_month)

    df = df.group_by("category").agg(pl.sum("total_sales")).sort("total_sales", descending=True).head(10)
    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["category"],
                y=df_pd["total_sales"],
                marker_color="purple",
            )
        ]
    )

    title = "Top 10 Categories"
    if selected_year and selected_month:
        title = f"Top 10 Categories - {selected_year}-{selected_month:02d}"
    elif selected_year:
        title = f"Top 10 Categories - {selected_year}"

    fig.update_layout(
        title=title,
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_region_chart():
    df = load_sales_by_region()

    if df.is_empty():
        return go.Figure()

    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["region"],
                y=df_pd["total_sales"],
                marker_color="teal",
            )
        ]
    )

    fig.update_layout(
        title="Sales by Region",
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
    )

    return fig


def create_category_chart():
    df = load_top_categories()

    if df.is_empty():
        return go.Figure()

    df_pd = df.to_pandas()

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_pd["category"],
                y=df_pd["total_sales"],
                marker_color="purple",
            )
        ]
    )

    fig.update_layout(
        title="Top Categories by Sales",
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def calculate_kpis():
    df = load_yearly_sales()

    if df.is_empty():
        return 0, 0, 0, 0

    total_sales = df["total_sales"].sum()
    total_transactions = df["num_transactions"].sum()
    avg_transaction = total_sales / total_transactions if total_transactions > 0 else 0
    num_years = len(df)

    return total_sales, total_transactions, avg_transaction, num_years


app.layout = dbc.Container(
    [
        html.H1("Retail Business Dashboard", className="text-center my-4"),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardBody(
                                [
                                    html.H4("Total Sales", className="card-title"),
                                    html.H2(id="kpi-total-sales", className="text-primary"),
                                ]
                            )
                        ]
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardBody(
                                [
                                    html.H4("Transactions", className="card-title"),
                                    html.H2(id="kpi-transactions", className="text-success"),
                                ]
                            )
                        ]
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardBody(
                                [
                                    html.H4("Avg Transaction", className="card-title"),
                                    html.H2(id="kpi-avg-transaction", className="text-info"),
                                ]
                            )
                        ]
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardBody(
                                [
                                    html.H4("Years", className="card-title"),
                                    html.H2(id="kpi-years", className="text-warning"),
                                ]
                            )
                        ]
                    ),
                    width=3,
                ),
            ],
            className="mb-4",
        ),
        dbc.Tabs(
            [
                dbc.Tab(
                    [
                        dcc.Graph(id="yearly-chart"),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Select Year:"),
                                        dcc.Dropdown(id="yearly-year-dropdown"),
                                    ],
                                    width=3,
                                )
                            ],
                            className="mb-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(dcc.Graph(id="yearly-region-chart"), width=6),
                                dbc.Col(dcc.Graph(id="yearly-category-chart"), width=6),
                            ]
                        ),
                    ],
                    label="Yearly Overview",
                ),
                dbc.Tab(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Select Year:"),
                                        dcc.Dropdown(id="monthly-year-dropdown"),
                                    ],
                                    width=3,
                                )
                            ],
                            className="mb-3",
                        ),
                        dcc.Graph(id="monthly-chart"),
                        dbc.Row(
                            [
                                dbc.Col(dcc.Graph(id="monthly-region-chart"), width=6),
                                dbc.Col(dcc.Graph(id="monthly-category-chart"), width=6),
                            ]
                        ),
                    ],
                    label="Monthly Drill-Down",
                ),
                dbc.Tab(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Select Year:"),
                                        dcc.Dropdown(id="weekly-year-dropdown"),
                                    ],
                                    width=3,
                                )
                            ],
                            className="mb-3",
                        ),
                        dcc.Graph(id="weekly-chart"),
                        dbc.Row(
                            [
                                dbc.Col(dcc.Graph(id="weekly-region-chart"), width=6),
                                dbc.Col(dcc.Graph(id="weekly-category-chart"), width=6),
                            ]
                        ),
                    ],
                    label="Weekly Drill-Down",
                ),
                dbc.Tab(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Select Year:"),
                                        dcc.Dropdown(id="daily-year-dropdown"),
                                    ],
                                    width=3,
                                ),
                                dbc.Col(
                                    [
                                        html.Label("Select Month:"),
                                        dcc.Dropdown(id="daily-month-dropdown"),
                                    ],
                                    width=3,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dcc.Graph(id="daily-chart"),
                        dbc.Row(
                            [
                                dbc.Col(dcc.Graph(id="daily-region-chart"), width=6),
                                dbc.Col(dcc.Graph(id="daily-category-chart"), width=6),
                            ]
                        ),
                    ],
                    label="Daily Drill-Down",
                ),
            ]
        ),
    ],
    fluid=True,
)


@app.callback(
    [
        Output("kpi-total-sales", "children"),
        Output("kpi-transactions", "children"),
        Output("kpi-avg-transaction", "children"),
        Output("kpi-years", "children"),
    ],
    Input("yearly-chart", "id"),
)
def update_kpis(_):
    total_sales, total_transactions, avg_transaction, num_years = calculate_kpis()
    return (
        f"€{total_sales:,.0f}",
        f"{total_transactions:,}",
        f"€{avg_transaction:.2f}",
        f"{num_years}",
    )


@app.callback(Output("yearly-chart", "figure"), Input("yearly-chart", "id"))
def update_yearly_chart(_):
    return create_yearly_chart()


@app.callback(
    Output("yearly-year-dropdown", "options"),
    Output("yearly-year-dropdown", "value"),
    Input("yearly-year-dropdown", "id"),
)
def populate_yearly_year_dropdown(_):
    df = load_yearly_sales()
    if df.is_empty():
        return [], None
    years = sorted(df["year"].unique().to_list())
    options = [{"label": str(year), "value": year} for year in years]
    return options, years[-1] if years else None


@app.callback(
    Output("yearly-region-chart", "figure"),
    Input("yearly-year-dropdown", "value"),
)
def update_yearly_region_chart(selected_year):
    return create_yearly_region_chart(selected_year)


@app.callback(
    Output("yearly-category-chart", "figure"),
    Input("yearly-year-dropdown", "value"),
)
def update_yearly_category_chart(selected_year):
    return create_yearly_category_chart(selected_year)


@app.callback(
    Output("monthly-year-dropdown", "options"),
    Output("monthly-year-dropdown", "value"),
    Input("monthly-year-dropdown", "id"),
)
def populate_monthly_year_dropdown(_):
    df = load_monthly_sales()
    if df.is_empty():
        return [], None
    years = sorted(df["year"].unique().to_list())
    options = [{"label": str(year), "value": year} for year in years]
    return options, years[-1] if years else None


@app.callback(
    Output("monthly-chart", "figure"),
    Input("monthly-year-dropdown", "value"),
)
def update_monthly_chart(selected_year):
    return create_monthly_chart(selected_year)


@app.callback(
    Output("monthly-region-chart", "figure"),
    Input("monthly-year-dropdown", "value"),
)
def update_monthly_region_chart(selected_year):
    return create_monthly_region_chart(selected_year)


@app.callback(
    Output("monthly-category-chart", "figure"),
    Input("monthly-year-dropdown", "value"),
)
def update_monthly_category_chart(selected_year):
    return create_monthly_category_chart(selected_year)


@app.callback(
    Output("weekly-year-dropdown", "options"),
    Output("weekly-year-dropdown", "value"),
    Input("weekly-year-dropdown", "id"),
)
def populate_weekly_year_dropdown(_):
    df = load_weekly_sales()
    if df.is_empty():
        return [], None
    years = sorted(df["iso_year"].unique().to_list())
    options = [{"label": str(year), "value": year} for year in years]
    return options, years[-1] if years else None


@app.callback(
    Output("weekly-chart", "figure"),
    Input("weekly-year-dropdown", "value"),
)
def update_weekly_chart(selected_year):
    return create_weekly_chart(selected_year)


@app.callback(
    Output("weekly-region-chart", "figure"),
    Input("weekly-year-dropdown", "value"),
)
def update_weekly_region_chart(selected_year):
    return create_weekly_region_chart(selected_year)


@app.callback(
    Output("weekly-category-chart", "figure"),
    Input("weekly-year-dropdown", "value"),
)
def update_weekly_category_chart(selected_year):
    return create_weekly_category_chart(selected_year)


@app.callback(
    Output("daily-year-dropdown", "options"),
    Output("daily-year-dropdown", "value"),
    Input("daily-year-dropdown", "id"),
)
def populate_daily_year_dropdown(_):
    df = load_daily_sales()
    if df.is_empty():
        return [], None
    years = sorted(df["year"].unique().to_list())
    options = [{"label": str(year), "value": year} for year in years]
    return options, years[-1] if years else None


@app.callback(
    Output("daily-month-dropdown", "options"),
    Output("daily-month-dropdown", "value"),
    Input("daily-year-dropdown", "value"),
)
def populate_daily_month_dropdown(selected_year):
    df = load_daily_sales()
    if df.is_empty() or not selected_year:
        return [], None
    df = df.filter(pl.col("year") == selected_year)
    months = sorted(df["month"].unique().to_list())
    month_names = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }
    options = [{"label": month_names[m], "value": m} for m in months]
    return options, months[-1] if months else None


@app.callback(
    Output("daily-chart", "figure"),
    Input("daily-year-dropdown", "value"),
    Input("daily-month-dropdown", "value"),
)
def update_daily_chart(selected_year, selected_month):
    return create_daily_chart(selected_year, selected_month)


@app.callback(
    Output("daily-region-chart", "figure"),
    Input("daily-year-dropdown", "value"),
    Input("daily-month-dropdown", "value"),
)
def update_daily_region_chart(selected_year, selected_month):
    return create_daily_region_chart(selected_year, selected_month)


@app.callback(
    Output("daily-category-chart", "figure"),
    Input("daily-year-dropdown", "value"),
    Input("daily-month-dropdown", "value"),
)
def update_daily_category_chart(selected_year, selected_month):
    return create_daily_category_chart(selected_year, selected_month)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
