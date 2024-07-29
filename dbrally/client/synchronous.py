# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
from elasticsearch import Elasticsearch
from elasticsearch.compat import warn_stacklevel
from elasticsearch.exceptions import (
    HTTP_EXCEPTIONS,
    ApiError,
    ElasticsearchWarning,
    UnsupportedProductError,
)

from dbrally.client.common import _WARNING_RE, _mimetype_header_to_compat, _quote_query
from dbrally.utils import versions


# This reproduces the product verification behavior of v7.14.0 of the client:
# https://github.com/elastic/elasticsearch-py/blob/v7.14.0/elasticsearch/transport.py#L606
#
# As of v8.0.0, the client determines whether the server is Elasticsearch by checking
# whether HTTP responses contain the `X-elastic-product` header. If they do not, it raises
# an `UnsupportedProductError`. This header was only introduced in Elasticsearch 7.14.0,
# however, so the client will consider any version of ES prior to 7.14.0 unsupported due to
# responses not including it.
#
# Because Rally needs to support versions of ES >= 6.8.0, we resurrect the previous
# logic for determining the authenticity of the server, which does not rely exclusively
# on this header.
class _ProductChecker:
    """Class which verifies we're connected to a supported product"""

    # States that can be returned from 'check_product'
    SUCCESS = True
    UNSUPPORTED_PRODUCT = 2
    UNSUPPORTED_DISTRIBUTION = 3

    @classmethod
    def raise_error(cls, state, meta, body):
        # These states mean the product_check() didn't fail so do nothing.
        if state in (None, True):
            return

        if state == cls.UNSUPPORTED_DISTRIBUTION:
            message = "The client noticed that the server is not a supported distribution of Elasticsearch"
        else:  # UNSUPPORTED_PRODUCT
            message = "The client noticed that the server is not Elasticsearch and we do not support this unknown product"
        raise UnsupportedProductError(message, meta=meta, body=body)

    @classmethod
    def check_product(cls, headers, response):
        # type: (dict[str, str], dict[str, str]) -> int
        """
        Verifies that the server we're talking to is Elasticsearch.
        Does this by checking HTTP headers and the deserialized
        response to the 'info' API. Returns one of the states above.
        """

        version = response.get("version", {})
        try:
            version_number = versions.Version.from_string(version.get("number", None))
        except TypeError:
            # No valid 'version.number' field, either Serverless Elasticsearch, or not Elasticsearch at all
            version_number = versions.Version.from_string("0.0.0")

        build_flavor = version.get("build_flavor", None)

        # Check all of the fields and headers for missing/valid values.
        try:
            bad_tagline = response.get("tagline", None) != "You Know, for Search"
            bad_build_flavor = build_flavor not in ("default", "serverless")
            bad_product_header = headers.get("x-elastic-product", None) != "Elasticsearch"
        except (AttributeError, TypeError):
            bad_tagline = True
            bad_build_flavor = True
            bad_product_header = True

        # 7.0-7.13 and there's a bad 'tagline' or unsupported 'build_flavor'
        if versions.Version.from_string("7.0.0") <= version_number < versions.Version.from_string("7.14.0"):
            if bad_tagline:
                return cls.UNSUPPORTED_PRODUCT
            elif bad_build_flavor:
                return cls.UNSUPPORTED_DISTRIBUTION

        elif (
            # No version or version less than 6.8.0, and we're not talking to a serverless elasticsearch
            (version_number < versions.Version.from_string("6.8.0") and not versions.is_serverless(build_flavor))
            # 6.8.0 and there's a bad 'tagline'
            or (versions.Version.from_string("6.8.0") <= version_number < versions.Version.from_string("7.0.0") and bad_tagline)
            # 7.14+ and there's a bad 'X-Elastic-Product' HTTP header
            or (versions.Version.from_string("7.14.0") <= version_number and bad_product_header)
        ):
            return cls.UNSUPPORTED_PRODUCT

        return True
