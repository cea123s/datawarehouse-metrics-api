"""
revenue.py — Revenue-related API endpoints.

All queries target the `gold.fact_sales` table in the data warehouse.
Parameterized queries are used wherever user input is involved to
prevent SQL injection.
"""

from fastapi import APIRouter, HTTPException, Query
from app.database import get_connection

router = APIRouter(tags=["Revenue"])


# ------------------------------------------------------------------ helpers
def _execute_query(sql: str, params: tuple = ()) -> list[dict]:
    """Run a read-only query and return rows as list of dicts."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except ConnectionError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query failed: {exc}")


# --------------------------------------------------------- 1. Total Revenue
@router.get("/revenue", summary="Total revenue")
def get_total_revenue(year: int | None = Query(None, description="Filter by year")):
    """
    Returns the total revenue (SUM of sales_amount).
    Optionally filtered by calendar year.
    """
    if year is not None:
        sql = """
            SELECT
                CAST(SUM(sales_amount) AS FLOAT) AS total_revenue
            FROM gold.fact_sales
            WHERE YEAR(order_date) = ?
        """
        rows = _execute_query(sql, (year,))
    else:
        sql = """
            SELECT
                CAST(SUM(sales_amount) AS FLOAT) AS total_revenue
            FROM gold.fact_sales
        """
        rows = _execute_query(sql)

    total = rows[0]["total_revenue"] if rows and rows[0]["total_revenue"] else 0.0
    return {"total_revenue": total, "year": year}


# -------------------------------------------------- 2. Revenue by Month
@router.get("/revenue-by-month", summary="Revenue grouped by month")
def get_revenue_by_month():
    """
    Returns revenue grouped by year and month, ordered chronologically.
    """
    sql = """
        SELECT
            YEAR(order_date)  AS year,
            MONTH(order_date) AS month,
            CAST(SUM(sales_amount) AS FLOAT) AS total_revenue
        FROM gold.fact_sales
        GROUP BY YEAR(order_date), MONTH(order_date)
        ORDER BY year, month
    """
    rows = _execute_query(sql)
    return {"data": rows}


# ------------------------------------------------- 3. Average Order Value
@router.get("/avg-order-value", summary="Average order value")
def get_avg_order_value():
    """
    Calculates: SUM(sales_amount) / COUNT(DISTINCT order_number).
    Returns 0 if there are no orders.
    """
    sql = """
        SELECT
            CASE
                WHEN COUNT(DISTINCT order_number) = 0 THEN 0
                ELSE CAST(
                    SUM(sales_amount) * 1.0 / COUNT(DISTINCT order_number)
                    AS FLOAT
                )
            END AS avg_order_value,
            COUNT(DISTINCT order_number) AS total_orders
        FROM gold.fact_sales
    """
    rows = _execute_query(sql)
    return rows[0] if rows else {"avg_order_value": 0.0, "total_orders": 0}


# ---------------------------------------------------- 4. Orders Count
@router.get("/orders-count", summary="Distinct order count")
def get_orders_count():
    """Returns the count of distinct orders."""
    sql = """
        SELECT
            COUNT(DISTINCT order_number) AS orders_count
        FROM gold.fact_sales
    """
    rows = _execute_query(sql)
    return rows[0] if rows else {"orders_count": 0}
