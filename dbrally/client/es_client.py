import warnings
from typing import Any, Iterable, Mapping, Optional

from elastic_transport import (
    ApiResponse,
    BinaryApiResponse,
    HeadApiResponse,
    ListApiResponse,
    ObjectApiResponse,
    TextApiResponse,
)

from elastic_transport.client_utils import DEFAULT
from elasticsearch import AsyncElasticsearch
from elasticsearch import Elasticsearch
from elasticsearch._async.client import IlmClient
from elasticsearch.compat import warn_stacklevel
from elasticsearch.exceptions import HTTP_EXCEPTIONS, ApiError, ElasticsearchWarning

from dbrally.client.common import _WARNING_RE, _mimetype_header_to_compat, _quote_query
from dbrally.client.context import RequestContextHolder
from dbrally.client.synchronous import _ProductChecker
from dbrally.utils import versions


class RallyIlmClient(IlmClient):
    async def put_lifecycle(self, *args, **kwargs):
        """
        The 'elasticsearch-py' 8.x method signature renames the 'policy' param to 'name', and the previously so-called
        'body' param becomes 'policy'
        """
        if args:
            kwargs["name"] = args[0]

        if body := kwargs.pop("body", None):
            kwargs["policy"] = body.get("policy", {})
        # pylint: disable=missing-kwoa
        return await IlmClient.put_lifecycle(self, **kwargs)


class RallyAsyncElasticsearch(AsyncElasticsearch,  RequestContextHolder):
    def __init__(self, *args, **kwargs):
        distribution_version = kwargs.pop("distribution_version", None)
        distribution_flavor = kwargs.pop("distribution_flavor", None)
        super().__init__(*args, **kwargs)
        # skip verification at this point; we've already verified this earlier with the synchronous client.
        # The async client is used in the hot code path and we use customized overrides (such as that we don't
        # parse response bodies in some cases for performance reasons, e.g. when using the bulk API).
        self._verified_elasticsearch = True
        self.distribution_version = distribution_version
        self.distribution_flavor = distribution_flavor

        # some ILM method signatures changed in 'elasticsearch-py' 8.x,
        # so we override method(s) here to provide BWC for any custom
        # runners that aren't using the new kwargs
        self.ilm = RallyIlmClient(self)

    @property
    def is_serverless(self):
        return versions.is_serverless(self.distribution_flavor)

    def options(self, *args, **kwargs):
        new_self = super().options(*args, **kwargs)
        new_self.distribution_version = self.distribution_version
        new_self.distribution_flavor = self.distribution_flavor
        return new_self

    async def perform_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[Any] = None,
    ) -> ApiResponse[Any]:
        # We need to ensure that we provide content-type and accept headers
        if body is not None:
            if headers is None:
                headers = {"content-type": "application/json", "accept": "application/json"}
            else:
                if headers.get("content-type") is None:
                    headers["content-type"] = "application/json"
                if headers.get("accept") is None:
                    headers["accept"] = "application/json"

        if headers:
            request_headers = self._headers.copy()
            request_headers.update(headers)
        else:
            request_headers = self._headers

        # Converts all parts of a Accept/Content-Type headers
        # from application/X -> application/vnd.elasticsearch+X
        # see https://github.com/elastic/elasticsearch/issues/51816
        # Not applicable to serverless
        if not self.is_serverless:
            if versions.is_version_identifier(self.distribution_version) and (
                versions.Version.from_string(self.distribution_version) >= versions.Version.from_string("8.0.0")
            ):
                _mimetype_header_to_compat("Accept", request_headers)
                _mimetype_header_to_compat("Content-Type", request_headers)

        if params:
            target = f"{path}?{_quote_query(params)}"
        else:
            target = path

        meta, resp_body = await self.transport.perform_request(
            method,
            target,
            headers=request_headers,
            body=body,
            request_timeout=self._request_timeout,
            max_retries=self._max_retries,
            retry_on_status=self._retry_on_status,
            retry_on_timeout=self._retry_on_timeout,
            client_meta=self._client_meta,
        )

        # HEAD with a 404 is returned as a normal response
        # since this is used as an 'exists' functionality.
        if not (method == "HEAD" and meta.status == 404) and (
            not 200 <= meta.status < 299
            and (self._ignore_status is DEFAULT or self._ignore_status is None or meta.status not in self._ignore_status)
        ):
            message = str(resp_body)

            # If the response is an error response try parsing
            # the raw Elasticsearch error before raising.
            if isinstance(resp_body, dict):
                try:
                    error = resp_body.get("error", message)
                    if isinstance(error, dict) and "type" in error:
                        error = error["type"]
                    message = error
                except (ValueError, KeyError, TypeError):
                    pass

            raise HTTP_EXCEPTIONS.get(meta.status, ApiError)(message=message, meta=meta, body=resp_body)

        # 'Warning' headers should be reraised as 'ElasticsearchWarning'
        if "warning" in meta.headers:
            warning_header = (meta.headers.get("warning") or "").strip()
            warning_messages: Iterable[str] = _WARNING_RE.findall(warning_header) or (warning_header,)
            stacklevel = warn_stacklevel()
            for warning_message in warning_messages:
                warnings.warn(
                    warning_message,
                    category=ElasticsearchWarning,
                    stacklevel=stacklevel,
                )

        if method == "HEAD":
            response = HeadApiResponse(meta=meta)
        elif isinstance(resp_body, dict):
            response = ObjectApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        elif isinstance(resp_body, list):
            response = ListApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        elif isinstance(resp_body, str):
            response = TextApiResponse(  # type: ignore[assignment]
                body=resp_body,
                meta=meta,
            )
        elif isinstance(resp_body, bytes):
            response = BinaryApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        else:
            response = ApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]

        return response


class RallySyncElasticsearch(Elasticsearch):
    def __init__(self, *args, **kwargs):
        distribution_version = kwargs.pop("distribution_version", None)
        distribution_flavor = kwargs.pop("distribution_flavor", None)
        super().__init__(*args, **kwargs)
        self._verified_elasticsearch = None
        self.distribution_version = distribution_version
        self.distribution_flavor = distribution_flavor

    @property
    def is_serverless(self):
        return versions.is_serverless(self.distribution_flavor)

    def options(self, *args, **kwargs):
        new_self = super().options(*args, **kwargs)
        new_self.distribution_version = self.distribution_version
        new_self.distribution_flavor = self.distribution_flavor
        return new_self

    def perform_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[Any] = None,
    ) -> ApiResponse[Any]:
        # We need to ensure that we provide content-type and accept headers
        if body is not None:
            if headers is None:
                headers = {"content-type": "application/json", "accept": "application/json"}
            else:
                if headers.get("content-type") is None:
                    headers["content-type"] = "application/json"
                if headers.get("accept") is None:
                    headers["accept"] = "application/json"

        if headers:
            request_headers = self._headers.copy()
            request_headers.update(headers)
        else:
            request_headers = self._headers

        if self._verified_elasticsearch is None:
            info = self.transport.perform_request(method="GET", target="/", headers=request_headers)
            info_meta = info.meta
            info_body = info.body

            if not 200 <= info_meta.status < 299:
                raise HTTP_EXCEPTIONS.get(info_meta.status, ApiError)(message=str(info_body), meta=info_meta, body=info_body)

            self._verified_elasticsearch = _ProductChecker.check_product(info_meta.headers, info_body)

            if self._verified_elasticsearch is not True:
                _ProductChecker.raise_error(self._verified_elasticsearch, info_meta, info_body)

        # Converts all parts of a Accept/Content-Type headers
        # from application/X -> application/vnd.elasticsearch+X
        # see https://github.com/elastic/elasticsearch/issues/51816
        if not self.is_serverless:
            if versions.is_version_identifier(self.distribution_version) and (
                versions.Version.from_string(self.distribution_version) >= versions.Version.from_string("8.0.0")
            ):
                _mimetype_header_to_compat("Accept", headers)
                _mimetype_header_to_compat("Content-Type", headers)

        if params:
            target = f"{path}?{_quote_query(params)}"
        else:
            target = path

        meta, resp_body = self.transport.perform_request(
            method,
            target,
            headers=request_headers,
            body=body,
            request_timeout=self._request_timeout,
            max_retries=self._max_retries,
            retry_on_status=self._retry_on_status,
            retry_on_timeout=self._retry_on_timeout,
            client_meta=self._client_meta,
        )

        # HEAD with a 404 is returned as a normal response
        # since this is used as an 'exists' functionality.
        if not (method == "HEAD" and meta.status == 404) and (
            not 200 <= meta.status < 299
            and (self._ignore_status is DEFAULT or self._ignore_status is None or meta.status not in self._ignore_status)
        ):
            message = str(resp_body)

            # If the response is an error response try parsing
            # the raw Elasticsearch error before raising.
            if isinstance(resp_body, dict):
                try:
                    error = resp_body.get("error", message)
                    if isinstance(error, dict) and "type" in error:
                        error = error["type"]
                    message = error
                except (ValueError, KeyError, TypeError):
                    pass

            raise HTTP_EXCEPTIONS.get(meta.status, ApiError)(message=message, meta=meta, body=resp_body)

        # 'Warning' headers should be reraised as 'ElasticsearchWarning'
        if "warning" in meta.headers:
            warning_header = (meta.headers.get("warning") or "").strip()
            warning_messages: Iterable[str] = _WARNING_RE.findall(warning_header) or (warning_header,)
            stacklevel = warn_stacklevel()
            for warning_message in warning_messages:
                warnings.warn(
                    warning_message,
                    category=ElasticsearchWarning,
                    stacklevel=stacklevel,
                )

        if method == "HEAD":
            response = HeadApiResponse(meta=meta)
        elif isinstance(resp_body, dict):
            response = ObjectApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        elif isinstance(resp_body, list):
            response = ListApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        elif isinstance(resp_body, str):
            response = TextApiResponse(  # type: ignore[assignment]
                body=resp_body,
                meta=meta,
            )
        elif isinstance(resp_body, bytes):
            response = BinaryApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        else:
            response = ApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]

        return response
