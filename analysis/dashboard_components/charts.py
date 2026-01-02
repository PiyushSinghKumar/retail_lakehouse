"""Chart creation functions for the dashboard."""

import plotly.graph_objects as go
import polars as pl


def apply_global_filter(df, filter_type, year=None, month=None, week=None):
    """Apply global filter to dataframe based on filter type."""
    if df.is_empty():
        return df

    if filter_type == "year" and year:
        if "year" in df.columns:
            df = df.filter(pl.col("year") == year)
        elif "iso_year" in df.columns:
            df = df.filter(pl.col("iso_year") == year)

    elif filter_type == "month" and year and month:
        if "year" in df.columns and "month" in df.columns:
            df = df.filter((pl.col("year") == year) & (pl.col("month") == month))

    elif filter_type == "week" and year and week:
        if "iso_year" in df.columns and "iso_week" in df.columns:
            df = df.filter((pl.col("iso_year") == year) & (pl.col("iso_week") == week))
        elif "date" in df.columns:
            # For daily data, derive iso_year and iso_week from date column
            df = df.filter(
                (pl.col("date").dt.iso_year() == year) &
                (pl.col("date").dt.week() == week)
            )

    return df


def get_filter_title(filter_type, year=None, month=None, week=None):
    """Generate title suffix based on active filter."""
    if filter_type == "month" and year and month:
        return f" - {year}-{month:02d}"
    elif filter_type == "week" and year and week:
        return f" - {year} Week {week}"
    elif filter_type == "year" and year:
        return f" - {year}"
    return ""


# ============================================================================
# OVERVIEW TAB CHARTS
# ============================================================================

def create_overview_yearly_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create yearly sales overview chart."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

    df_pd = df.to_pandas()

    # Use bar chart if single value, line chart for multiple years
    if len(df_pd) == 1:
        fig = go.Figure(
            data=[
                go.Bar(
                    x=df_pd["year"].astype(str),
                    y=df_pd["total_sales"],
                    marker_color="royalblue",
                    text=df_pd["total_sales"].apply(lambda x: f"€{x:,.0f}"),
                    textposition="outside",
                )
            ]
        )
    else:
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

    title = "Yearly Sales" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Year",
        yaxis_title="Total Sales (€)",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_overview_region_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create sales by region chart for overview."""
    df = apply_global_filter(df, filter_type, year, month, week)

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

    title = "Sales by Region" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_overview_category_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create top categories chart for overview."""
    df = apply_global_filter(df, filter_type, year, month, week)

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

    title = "Top 10 Categories" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


# ============================================================================
# YEARLY DRILL-DOWN TAB CHARTS
# ============================================================================

def create_yearly_drill_chart(df_yearly, df_monthly, filter_type="default", year=None, month=None, week=None):
    """Create yearly drill-down chart.

    If year is selected, shows monthly breakdown for that year.
    Otherwise, shows year-by-year trend.
    """
    # If year is selected, show monthly breakdown
    if year:
        df = df_monthly.filter(pl.col("year") == year)

        if df.is_empty():
            return go.Figure()

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

        title = f"Monthly Sales - {year}"
        xaxis_title = "Month"
    else:
        # Show yearly trend
        df = df_yearly

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

        title = "Yearly Sales Trend"
        xaxis_title = "Year"

    fig.update_layout(
        title=title,
        xaxis_title=xaxis_title,
        yaxis_title="Total Sales (€)",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_yearly_region_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create yearly sales by region chart."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

    # Aggregate by region
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

    title = "Sales by Region" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_yearly_category_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create yearly sales by category chart."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

    # Aggregate by category and get top 10
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

    title = "Top 10 Categories" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


# ============================================================================
# MONTHLY DRILL-DOWN TAB CHARTS (Shows daily breakdown for selected month)
# ============================================================================

def create_monthly_drill_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create monthly drill-down chart (shows daily sales for the month)."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

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

    title = "Daily Sales" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Total Sales (€)",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_monthly_region_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create monthly sales by region chart."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

    # Aggregate by region
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

    title = "Sales by Region" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_monthly_category_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create monthly sales by category chart."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

    # Aggregate by category and get top 10
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

    title = "Top 10 Categories" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


# ============================================================================
# WEEKLY DRILL-DOWN TAB CHARTS (Shows daily breakdown for selected week)
# ============================================================================

def create_weekly_drill_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create weekly drill-down chart (shows daily sales for the week)."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

    df_pd = df.to_pandas()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_pd["date"],
            y=df_pd["total_sales"],
            mode="lines+markers",
            name="Total Sales",
            line=dict(color="green", width=2),
            marker=dict(size=5),
        )
    )

    title = "Daily Sales" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Total Sales (€)",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_weekly_region_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create weekly sales by region chart."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

    # Aggregate by region
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

    title = "Sales by Region" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Region",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig


def create_weekly_category_chart(df, filter_type="default", year=None, month=None, week=None):
    """Create weekly sales by category chart."""
    df = apply_global_filter(df, filter_type, year, month, week)

    if df.is_empty():
        return go.Figure()

    # Aggregate by category and get top 10
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

    title = "Top 10 Categories" + get_filter_title(filter_type, year, month, week)

    fig.update_layout(
        title=title,
        xaxis_title="Category",
        yaxis_title="Total Sales (€)",
        height=400,
        xaxis_tickangle=-45,
    )

    return fig
