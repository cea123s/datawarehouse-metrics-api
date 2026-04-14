"""
products.py — Product-related API endpoints.

Queries join 'gold.fact_sales' with 'gold.dim_products' to surface product-level
business metrics.  Parameterized queries are used wherever user input
is involved to prevent SQL injection.
"""

from fastapi import APIRouter, HTTPException, Query
from app.database import get_connection

router = APIRouter(tags=["Products"])


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


# ------------------------------------------------ 5. Top Products by Revenue
@router.get("/top-products", summary="Top N products by revenue")
def get_top_products(
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
):
    """
    Returns the top N products ranked by total revenue.

    Each row includes:
      • product_name
      • total_revenue  (SUM of sales_amount)
      • total_quantity  (SUM of quantity)

    The `limit` query parameter controls how many products are returned
    (default 10, max 100).  Parameterized to prevent SQL injection.
    """
    # NOTE: TOP with a parameter requires dynamic SQL in T-SQL, but
    # pyodbc supports parameterised TOP directly.
    sql = """
        SELECT TOP (?)
            dp.product_name,
            CAST(SUM(fs.sales_amount) AS FLOAT)  AS total_revenue,
            SUM(fs.quantity)                       AS total_quantity
        FROM gold.fact_sales  AS fs
        INNER JOIN gold.dim_products AS dp
            ON fs.product_key = dp.product_key
        GROUP BY dp.product_name
        ORDER BY total_revenue DESC
    """
    rows = _execute_query(sql, (limit,))
    return {"limit": limit, "data": rows}


# ------------------------------------------- 6. Revenue by Product Category
@router.get("/revenue-by-category", summary="Revenue by product category")
def get_revenue_by_category():
    """
    Returns total revenue grouped by product category,
    ordered from highest to lowest.
    """
    sql = """
        SELECT
            dp.category,
            CAST(SUM(fs.sales_amount) AS FLOAT) AS total_revenue
        FROM gold.fact_sales  AS fs
        INNER JOIN gold.dim_products AS dp
            ON fs.product_key = dp.product_key
        GROUP BY dp.category
        ORDER BY total_revenue DESC
    """
    rows = _execute_query(sql)
    return {"data": rows}
