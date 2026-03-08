"""
main.py — FastAPI application entry point
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import agents, auth, catalog, config_routes, connection, conversion, pipeline, query_logs, validation

app = FastAPI(
    title="MigData API",
    version="1.0.0",
    description="Data Migration Intelligence Platform API",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routes
app.include_router(auth.router)
app.include_router(config_routes.router)
app.include_router(connection.router)
app.include_router(pipeline.router)
app.include_router(catalog.router)
app.include_router(conversion.router)
app.include_router(validation.router)
app.include_router(query_logs.router)
app.include_router(agents.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}
