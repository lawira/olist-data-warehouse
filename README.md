# Retail Data Warehouse: Olist E-Commerce

Built a retail data warehouse for the Olist Brazilian e-commerce dataset using Medallion Architecture and Galaxy Schema, modeling 9 source tables into 8 analytical views.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Dataset](#dataset)
- [Architecture](#architecture)
- [Schema Design](#schema-design)
- [Data Layers](#data-layers)
- [Gold Layer Views](#gold-layer-views)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)

---

## Project Overview

This project demonstrates end-to-end data warehouse design using the Olist Brazilian e-commerce public dataset. The architecture follows the Medallion pattern — Bronze for raw ingestion, Silver for cleaning and validation, and Gold for analytical consumption. The Gold layer implements a Galaxy Schema with conformed dimensions, surrogate keys, and date intelligence, enabling multi-dimensional analysis across orders, products, sellers, customers, and payments.

---

## Dataset

[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — a real commercial dataset from Olist, the largest department store in Brazilian marketplaces.

### Source Tables (9):

| Table | Description |
|---|---|
| `olist_orders_dataset` | Customer orders |
| `olist_order_items_dataset` | Items purchased per order |
| `olist_order_payments_dataset` | Payment transactions |
| `olist_order_reviews_dataset` | Customer reviews |
| `olist_customers_dataset` | Customer information |
| `olist_sellers_dataset` | Seller information |
| `olist_products_dataset` | Product information |
| `product_category_name_translation` | Product category translation (PT → EN) |
| `olist_geolocation_dataset` | Zip code geolocation reference |

---

## Architecture

This project follows the **Medallion Architecture** — a three-layer data pipeline that progressively refines raw data into analytics-ready views.

```
🥉 BRONZE LAYER          🥈 SILVER LAYER           🥇 GOLD LAYER
─────────────────         ──────────────────         ──────────────────
Raw ingestion             Cleaning &                 Analytical views
No transformations        validation                 Galaxy Schema
9 source tables    →      Flagging &          →      4 fact tables
                          deduplication              4 dimension tables
                          Business rules             Surrogate keys
```

---

## Schema Design

The Gold layer implements a **Galaxy Schema** (Fact Constellation Schema) — multiple fact tables sharing conformed dimension tables.

```
                         dim_customers
                              │
          dim_dates ────── fact_orders ────── fact_order_items ──── dim_products
                              │                                  └── dim_sellers
                              ├── fact_payments
                              └── fact_reviews
```

### Fact Tables:

| Table | Granularity | Key Measures |
|---|---|---|
| `fact_orders` | One row per order | `delivery_days`, `delay_days` |
| `fact_order_items` | One row per item per order | `price`, `freight_value` |
| `fact_payments` | One row per payment per order | `payment_value`, `payment_installments` |
| `fact_reviews` | One row per review | `review_score` |

### Dimension Tables:

| Table | Description | Surrogate Key |
|---|---|---|
| `dim_customers` | Customer information with location | `customer_key` |
| `dim_sellers` | Seller information with location | `seller_key` |
| `dim_products` | Product details with English category | `product_key` |
| `dim_dates` | Calendar dimension (2016–2018) | `date_key` (YYYYMMDD) |

---

## Data Layers

### 🥉 Bronze Layer
- Raw ingestion of all 9 source tables
- No transformations applied
- Data preserved exactly as received

### 🥈 Silver Layer
- Data type standardization
- Null handling and imputation
- Duplicate detection and flagging
- Outlier identification
- Business rule validation
- Date column validation
- `dim_dates` physical table created here (2016–2018)

### 🥇 Gold Layer
- Views only — no physical tables (except `dim_dates` in Silver)
- Surrogate keys generated using `ROW_NUMBER()` on static dataset
- Date columns converted from `DATETIME2` to `INT` (YYYYMMDD) for `dim_dates` joins
- Silver data quality flags used to filter — never exposed
- Conformed dimensions shared across all fact tables

---

## Gold Layer Views

### Dimension Views:

```sql
-- dim_customers: customers enriched with location
-- dim_sellers: sellers enriched with location
-- dim_products: products with English category translation
-- dim_dates: calendar dimension
```

### Fact Views:

```sql
-- fact_orders: central fact — order level metrics
-- fact_order_items: item level — price and freight
-- fact_payments: payment transactions per order
-- fact_reviews: customer feedback per order
```

### Key Design Decisions:

| Decision | Reason |
|---|---|
| Views only in Gold | Flexibility — easy to update without data migration |
| `ROW_NUMBER()` for surrogate keys | Safe for static Olist dataset |
| `date_key` as YYYYMMDD INT | Fast integer joins with `dim_dates` |
| `fact_orders` has own `order_key` | Child facts (`payments`, `reviews`, `items`) join to it |
| Geolocation excluded from dimensions | Use by app only |
| Silver flags used in WHERE, not SELECT | Gold exposes only clean, trusted data |

---

## Tech Stack

| Tool | Usage |
|---|---|
| **SQL Server** | Data warehouse platform |
| **Medallion Architecture** | Data pipeline pattern |
| **Galaxy Schema** | Dimensional modeling approach |
| **Kimball Methodology** | Dimensional modeling best practices |

---

## Project Structure

```
├── bronze/
│   └── raw source table ingestion scripts
├── silver/
│   ├── cleaning and validation scripts
│   ├── flagging and deduplication scripts
│   └── dim_dates population script
└── gold/
    ├── dim_customers.sql
    ├── dim_sellers.sql
    ├── dim_products.sql
    ├── dim_dates.sql
    ├── fact_orders.sql
    ├── fact_order_items.sql
    ├── fact_payments.sql
    └── fact_reviews.sql
```

---

## Acknowledgements

- [Olist](https://olist.com/) for providing the public dataset
- [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) for hosting the dataset
- [Ralph Kimball](https://www.kimballgroup.com/) for dimensional modeling methodology

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

---

## 🤖 About Me

Hi there! I'm **Ari Wira Putra**, also known as **Wira**. I’m a Data Architect.
