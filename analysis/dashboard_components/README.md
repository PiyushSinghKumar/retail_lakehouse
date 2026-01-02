# Dashboard Components

This directory contains the modular components for the retail analytics dashboard.

## Structure

```
dashboard_components/
├── __init__.py           # Package initialization
├── data_loaders.py       # Data loading functions
├── charts.py             # Chart creation functions
├── filters.py            # Global filter component
├── layout.py             # Dashboard layout
├── callbacks.py          # Dash callbacks for interactivity
└── README.md             # This file
```

## Global Filter

The dashboard uses a **single global filter** at the top that applies to ALL tabs:

- **Default**: Shows all available data
- **Year**: Filters all data to the selected year
- **Month**: Filters all data to the selected year and month
- **Week**: Filters all data to the selected ISO year and week

## Tabs

### 1. Overview
- KPIs and summary charts
- Shows aggregated yearly trends

### 2. Yearly Drill-Down
- Data aggregated by year
- Regional and category breakdowns

### 3. Monthly Drill-Down
- Shows **daily sales** for the selected month
- When month filter is active, displays day-by-day breakdown
- Regional and category breakdowns for the period

### 4. Weekly Drill-Down
- Shows **daily sales** for the selected week
- When week filter is active, displays day-by-day breakdown
- Regional and category breakdowns for the period

## How Filtering Works

The `apply_global_filter()` function in `charts.py` handles filtering logic:

1. Checks the active filter type
2. Applies appropriate column filters (year, month, week)
3. Returns the filtered dataframe

All charts receive:
- `filter_type`: "default", "year", "month", or "week"
- `year`: Selected year (if applicable)
- `month`: Selected month (if applicable)
- `week`: Selected week (if applicable)

## Adding New Charts

To add a new chart:

1. Create the chart function in `charts.py`
2. Add it to the appropriate tab layout in `layout.py`
3. Register a callback in `callbacks.py` with the global filter inputs
4. Ensure the chart uses `apply_global_filter()` for consistency
