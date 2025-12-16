"""
API entrypoint.

Keep this file focused on HTTP concerns (routing, auth wiring, request/response).
The heavy lifting lives in:
- arkham_api.storage (parquet reads + cache)
- arkham_api.query   (filtering + pagination)
- arkham_api.refresh (connector + atomic refresh)
"""

from __future__ import annotations

import hashlib
import logging
import threading
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials

from arkham_api import auth, query, refresh, storage
from arkham_api.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):  # noqa: ARG001
        # Safe-ish diagnostics to confirm env is being read (does not print the token).
        if settings.api_token:
            token_hash = hashlib.sha256(settings.api_token.encode("utf-8")).hexdigest()[
                :10
            ]
            logging.info(
                "Auth enabled (API_TOKEN sha256[:10]=%s, len=%d)",
                token_hash,
                len(settings.api_token),
            )
        else:
            logging.info("Auth disabled (API_TOKEN not set)")
        logging.info(
            "RAW_PATH=%s MODELED_DIR=%s", settings.raw_path, settings.modeled_dir
        )
        yield

    app = FastAPI(title="Arkham Challenge API", version="0.1.0", lifespan=lifespan)

    # In-process concurrency controls + cache.
    app.state.lock = threading.Lock()
    app.state.cache: storage.Cache | None = None

    @app.get("/data")
    def get_data(
        credentials: HTTPAuthorizationCredentials | None = Depends(auth.bearer),
        page: int = Query(1, ge=1),
        limit: int = Query(100, ge=1, le=1000),
        start_date: str | None = Query(
            None, description="Filter by outage start date (YYYY-MM-DD)"
        ),
        end_date: str | None = Query(
            None, description="Filter by outage start date (YYYY-MM-DD)"
        ),
        facility_id: str | None = Query(None),
        generator: str | None = Query(None),
        plant_key: int | None = Query(None, ge=1),
        plant_name: str | None = Query(
            None, description="Case-insensitive substring match"
        ),
    ) -> dict[str, Any]:
        auth.require_auth(settings, credentials)

        # Cache the joined view in-memory, keyed by parquet mtimes.
        with app.state.lock:
            cache: storage.Cache | None = app.state.cache
            try:
                mtimes = storage.modeled_mtimes(settings.modeled_dir)
            except FileNotFoundError as e:
                raise storage.modeled_not_found() from e

            if cache is None or cache.mtimes != mtimes:
                view, mtimes = storage.load_outage_view(settings.modeled_dir)
                cache = storage.Cache(mtimes=mtimes, outage_view=view)
                app.state.cache = cache

            base_df = cache.outage_view

        df = base_df
        try:
            df = query.apply_filters(
                df,
                facility_id=facility_id,
                generator=generator,
                plant_key=plant_key,
                plant_name=plant_name,
                start_date=start_date,
                end_date=end_date,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        df = df.sort_values(
            ["OutageStartTimestamp", "PlantKey"], ascending=[False, True]
        )
        page_df, total = query.paginate(df, page=page, limit=limit)
        return {
            "page": page,
            "limit": limit,
            "total": total,
            "data": page_df.to_dict(orient="records"),
        }

    @app.post("/refresh")
    def refresh_data(
        credentials: HTTPAuthorizationCredentials | None = Depends(auth.bearer),
        preview: bool = Query(
            False, description="Include a small preview of the modeled tables"
        ),
        head: int = Query(
            5, ge=1, le=100, description="Rows to include per table when preview=true"
        ),
    ) -> dict[str, Any]:
        auth.require_auth(settings, credentials)
        try:
            # Run refresh without holding the lock so /data can keep serving cached/old
            # data. Only lock around the atomic directory swap + cache invalidation.
            tmp_dir, payload = refresh.run_refresh_build_tmp(
                raw_path=settings.raw_path,
                modeled_dir=settings.modeled_dir,
                preview=preview,
                head=head,
            )
            with app.state.lock:
                refresh.atomic_replace_modeled_dir(
                    src_dir=tmp_dir, dst_dir=settings.modeled_dir
                )
                app.state.cache = None
            return payload
        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(e)) from e

    return app


app = create_app()
