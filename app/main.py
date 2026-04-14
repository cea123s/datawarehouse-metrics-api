"""
main.py — Application entry point for the Data Warehouse Backend API.

Responsibilities:
  • Validate required environment variables on startup.
  • Register route modules (revenue, products).
  • Expose a health-check root endpoint.
  • Enable auto-generated interactive docs at /docs.
"""

from fastapi import FastAPI
from app.database import _validate_env
from app.routes import revenue, products

# -------------------------------------------------------------------
# Validate environment on import so the app fails fast if credentials
# are missing, rather than at the first database call.
# -------------------------------------------------------------------
_validate_env()

# -------------------------------------------------------------------
# FastAPI application instance
# -------------------------------------------------------------------
app = FastAPI(
    title="Data Warehouse API",
    description=(
        "REST API that exposes business metrics from a SQL Server data warehouse. "
        "Provides revenue analytics, order statistics, and product performance data. "
        "Build by Cristian Añon."
    ),
    version="1.0.0",
    docs_url="/docs",       # Swagger UI  (enabled by default)
    redoc_url="/redoc",     # ReDoc       (enabled by default)
)

# -------------------------------------------------------------------
# Register routers
# -------------------------------------------------------------------
app.include_router(revenue.router)
app.include_router(products.router)


# -------------------------------------------------------------------
# Root / Health-check endpoint
# -------------------------------------------------------------------
@app.get("/", tags=["Health"], summary="Health check")
def root():
    """Basic health-check endpoint to verify the API is running."""
    return {
        "status": "ok",
        "message": "Data Warehouse API is running.",
        "docs": "/docs",
    }
