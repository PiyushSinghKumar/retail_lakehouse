# Lakehouse Architecture Diagram

Here is the visual representation of the Medallion Architecture described in Article 1.

## Mermaid Diagram

```mermaid
flowchart LR
    %% Nodes
    Generator[("Data Generator<br/>(Faker + NumPy)")]
    
    subgraph Storage ["Local Filesystem"]
        direction TB
        Bronze[("Bronze Layer<br/>Raw Parquet<br/>~1.2 GB")]
        Silver[("Silver Layer<br/>Cleaned Parquet<br/>~1.3 GB")]
        Gold[("Gold Layer<br/>Aggregated Parquet<br/>~50 MB")]
    end
    
    Dashboard["Interactive Dashboard<br/>(Dash + Plotly)"]

    %% Connections
    Generator -->|"Generate (Zstd)"| Bronze
    Bronze -->|"Polars (Clean/Dedupe)"| Silver
    Silver -->|"Polars (Aggregate)"| Gold
    Gold -->|"Read (Instant)"| Dashboard

    %% Styling
    style Bronze fill:#cd7f32,stroke:#333,stroke-width:2px,color:black
    style Silver fill:#e0e0e0,stroke:#333,stroke-width:2px,color:black
    style Gold fill:#ffd700,stroke:#333,stroke-width:2px,color:black
    style Generator fill:#f9f,stroke:#333,stroke-width:2px,color:black
    style Dashboard fill:#9cf,stroke:#333,stroke-width:2px,color:black
```

## ASCII Version

```
+-----------------+       +------------------+       +------------------+       +----------------+
|                 |       |                  |       |                  |       |                |
|  Data Generator |------>|   Bronze Layer   |------>|   Silver Layer   |------>|   Gold Layer   |
| (Faker + NumPy) | Write |   (Raw Parquet)  | Clean | (Cleaned Parquet)| Agg   | (Aggregations) |
|                 |       |     ~1.2 GB      |       |      ~1.3 GB     |       |     ~50 MB     |
+-----------------+       +------------------+       +------------------+       +-------+--------+
                                                                                        |
                                                                                        | Read
                                                                                        v
                                                                               +----------------+
                                                                               |                |
                                                                               |  Dash App UI   |
                                                                               | (Fast Loading) |
                                                                               |                |
                                                                               +----------------+
```