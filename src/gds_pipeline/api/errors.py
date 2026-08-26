from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from gds_pipeline.api.models import ErrorDetail, ErrorResponse


class ApiError(Exception):
    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def error_content(
    *,
    code: str,
    message: str,
) -> dict[str, object]:
    response = ErrorResponse(
        error=ErrorDetail(
            code=code,
            message=message,
        )
    )
    return response.model_dump()


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(
        _request: Request,
        error: ApiError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=error.status_code,
            content=error_content(
                code=error.code,
                message=error.message,
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_error(
        _request: Request,
        error: StarletteHTTPException,
    ) -> JSONResponse:
        if error.status_code == 404:
            code = "NOT_FOUND"
            message = "Resource not found"
        else:
            code = "HTTP_ERROR"
            message = "HTTP request failed"

        return JSONResponse(
            status_code=error.status_code,
            content=error_content(
                code=code,
                message=message,
            ),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _request: Request,
        _error: RequestValidationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=error_content(
                code="INVALID_REQUEST",
                message="Request validation failed",
            ),
        )

    @app.exception_handler(Exception)
    async def handle_internal_error(
        _request: Request,
        _error: Exception,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content=error_content(
                code="INTERNAL_SERVER_ERROR",
                message="An unexpected error occurred",
            ),
        )
