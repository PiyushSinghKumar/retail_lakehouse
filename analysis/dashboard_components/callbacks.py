"""Dashboard callbacks for interactivity."""

from dash import Input, Output
import polars as pl

from . import data_loaders as dl
from . import charts


def register_callbacks(app, aggregated_dir):
    """Register all dashboard callbacks."""

    # ========================================================================
    # TAB-BASED FILTER VISIBILITY
    # ========================================================================

    @app.callback(
        [
            Output("year-filter-col", "style"),
            Output("month-filter-col", "style"),
            Output("week-filter-col", "style"),
        ],
        Input("tabs", "active_tab"),
    )
    def update_filter_visibility(active_tab):
        """Show/hide filters based on active tab."""
        # Overview: No filters
        if active_tab == "overview":
            return {"display": "none"}, {"display": "none"}, {"display": "none"}
        # Yearly: Only year filter
        elif active_tab == "yearly":
            return {"display": "block"}, {"display": "none"}, {"display": "none"}
        # Monthly: Year + Month filters
        elif active_tab == "monthly":
            return {"display": "block"}, {"display": "block"}, {"display": "none"}
        # Weekly: Year + Week filters
        elif active_tab == "weekly":
            return {"display": "block"}, {"display": "none"}, {"display": "block"}
        return {"display": "none"}, {"display": "none"}, {"display": "none"}

    # ========================================================================
    # KPI CALCULATIONS
    # ========================================================================

    @app.callback(
        [
            Output("kpi-total-sales", "children"),
            Output("kpi-total-transactions", "children"),
            Output("kpi-avg-transaction", "children"),
            Output("kpi-years", "children"),
        ],
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
            Input("global-month", "value"),
            Input("global-week", "value"),
        ],
    )
    def update_kpis(active_tab, year, month, week):
        # Determine filter type from active tab
        filter_type = "default"
        if year:
            filter_type = "year"
        if active_tab == "monthly" and month:
            filter_type = "month"
        elif active_tab == "weekly" and week:
            filter_type = "week"

        df = dl.load_yearly_sales(aggregated_dir)
        if df.is_empty():
            return "€0", "0", "€0.00", "0"

        # Apply filter
        df = charts.apply_global_filter(df, filter_type, year, month, week)

        total_sales = df["total_sales"].sum()
        total_transactions = df["num_transactions"].sum()
        avg_transaction = total_sales / total_transactions if total_transactions > 0 else 0
        num_years = df["year"].n_unique()

        return (
            f"€{total_sales:,.0f}",
            f"{total_transactions:,}",
            f"€{avg_transaction:.2f}",
            f"{num_years}",
        )

    # ========================================================================
    # FILTER DROPDOWNS POPULATION
    # ========================================================================

    @app.callback(
        [
            Output("global-year", "options"),
            Output("global-year", "value"),
        ],
        Input("tabs", "active_tab"),
    )
    def populate_year_dropdown(active_tab):
        df = dl.load_yearly_sales(aggregated_dir)
        if df.is_empty():
            return [], None

        years = sorted(df["year"].unique().to_list())
        options = [{"label": str(year), "value": year} for year in years]

        # Default to latest year only for yearly/monthly/weekly tabs, None for overview
        if active_tab == "overview":
            return options, None
        else:
            latest_year = years[-1] if years else None
            return options, latest_year

    @app.callback(
        [
            Output("global-month", "options"),
            Output("global-month", "value"),
        ],
        [Input("tabs", "active_tab"), Input("global-year", "value")],
    )
    def populate_month_dropdown(active_tab, year):
        if active_tab != "monthly" or not year:
            return [], None

        df = dl.load_monthly_sales(aggregated_dir)
        if df.is_empty():
            return [], None

        df = df.filter(pl.col("year") == year)
        months = sorted(df["month"].unique().to_list())

        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December",
        }

        options = [{"label": month_names[m], "value": m} for m in months]
        # Default to latest month
        latest_month = months[-1] if months else None
        return options, latest_month

    @app.callback(
        [
            Output("global-week", "options"),
            Output("global-week", "value"),
        ],
        [Input("tabs", "active_tab"), Input("global-year", "value")],
    )
    def populate_week_dropdown(active_tab, year):
        # Only populate weeks when on weekly tab AND a year is selected
        if active_tab != "weekly" or not year:
            return [], None

        df = dl.load_weekly_sales(aggregated_dir)
        if df.is_empty():
            return [], None

        # Filter by the selected year
        df = df.filter(pl.col("iso_year") == year)
        weeks = sorted(df["iso_week"].unique().to_list())

        options = [{"label": f"Week {w}", "value": w} for w in weeks]
        # Default to latest week
        latest_week = weeks[-1] if weeks else None
        return options, latest_week

    # ========================================================================
    # OVERVIEW TAB CALLBACKS
    # ========================================================================

    @app.callback(
        Output("overview-yearly-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
        ],
    )
    def update_overview_yearly(active_tab, year):
        filter_type = "year" if year else "default"
        df = dl.load_yearly_sales(aggregated_dir)
        return charts.create_overview_yearly_chart(df, filter_type, year, None, None)

    @app.callback(
        Output("overview-region-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
        ],
    )
    def update_overview_region(active_tab, year):
        filter_type = "year" if year else "default"
        df = dl.load_sales_by_region(aggregated_dir)
        return charts.create_overview_region_chart(df, filter_type, year, None, None)

    @app.callback(
        Output("overview-category-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
        ],
    )
    def update_overview_category(active_tab, year):
        filter_type = "year" if year else "default"
        df = dl.load_top_categories(aggregated_dir)
        return charts.create_overview_category_chart(df, filter_type, year, None, None)

    # ========================================================================
    # YEARLY DRILL-DOWN TAB CALLBACKS
    # ========================================================================

    @app.callback(
        Output("yearly-drill-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
        ],
    )
    def update_yearly_drill(active_tab, year):
        filter_type = "year" if year else "default"
        df_yearly = dl.load_yearly_sales(aggregated_dir)
        df_monthly = dl.load_monthly_sales(aggregated_dir)
        return charts.create_yearly_drill_chart(df_yearly, df_monthly, filter_type, year, None, None)

    @app.callback(
        Output("yearly-region-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
        ],
    )
    def update_yearly_region(active_tab, year):
        filter_type = "year" if year else "default"
        df = dl.load_yearly_by_region(aggregated_dir)
        return charts.create_yearly_region_chart(df, filter_type, year, None, None)

    @app.callback(
        Output("yearly-category-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
        ],
    )
    def update_yearly_category(active_tab, year):
        filter_type = "year" if year else "default"
        df = dl.load_yearly_by_category(aggregated_dir)
        return charts.create_yearly_category_chart(df, filter_type, year, None, None)

    # ========================================================================
    # MONTHLY DRILL-DOWN TAB CALLBACKS (Shows daily breakdown)
    # ========================================================================

    @app.callback(
        Output("monthly-drill-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
            Input("global-month", "value"),
        ],
    )
    def update_monthly_drill(active_tab, year, month):
        filter_type = "default"
        if year and month:
            filter_type = "month"
        elif year:
            filter_type = "year"

        df = dl.load_daily_sales(aggregated_dir)
        return charts.create_monthly_drill_chart(df, filter_type, year, month, None)

    @app.callback(
        Output("monthly-region-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
            Input("global-month", "value"),
        ],
    )
    def update_monthly_region(active_tab, year, month):
        filter_type = "default"
        if year and month:
            filter_type = "month"
        elif year:
            filter_type = "year"

        df = dl.load_daily_by_region(aggregated_dir)
        return charts.create_monthly_region_chart(df, filter_type, year, month, None)

    @app.callback(
        Output("monthly-category-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
            Input("global-month", "value"),
        ],
    )
    def update_monthly_category(active_tab, year, month):
        filter_type = "default"
        if year and month:
            filter_type = "month"
        elif year:
            filter_type = "year"

        df = dl.load_daily_by_category(aggregated_dir)
        return charts.create_monthly_category_chart(df, filter_type, year, month, None)

    # ========================================================================
    # WEEKLY DRILL-DOWN TAB CALLBACKS (Shows daily breakdown)
    # ========================================================================

    @app.callback(
        Output("weekly-drill-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
            Input("global-week", "value"),
        ],
    )
    def update_weekly_drill(active_tab, year, week):
        filter_type = "default"
        if year and week:
            filter_type = "week"
        elif year:
            filter_type = "year"

        df = dl.load_daily_sales(aggregated_dir)
        return charts.create_weekly_drill_chart(df, filter_type, year, None, week)

    @app.callback(
        Output("weekly-region-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
            Input("global-week", "value"),
        ],
    )
    def update_weekly_region(active_tab, year, week):
        filter_type = "default"
        if year and week:
            filter_type = "week"
        elif year:
            filter_type = "year"

        df = dl.load_daily_by_region(aggregated_dir)
        return charts.create_weekly_region_chart(df, filter_type, year, None, week)

    @app.callback(
        Output("weekly-category-chart", "figure"),
        [
            Input("tabs", "active_tab"),
            Input("global-year", "value"),
            Input("global-week", "value"),
        ],
    )
    def update_weekly_category(active_tab, year, week):
        filter_type = "default"
        if year and week:
            filter_type = "week"
        elif year:
            filter_type = "year"

        df = dl.load_daily_by_category(aggregated_dir)
        return charts.create_weekly_category_chart(df, filter_type, year, None, week)
