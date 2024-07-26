import logging

from dbrally.client.pg_client import RallyAsyncPostgres


class PgClientFactory:
    """
    Abstracts how the Postgres client is created. Intended for testing.
    """

    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = dict(client_options)
        self.ssl_context = None
        self.logger = logging.getLogger(__name__)
        self.aws_log_in_dict = {}

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        # if "amazon_aws_log_in" in masked_client_options:
        #     self.aws_log_in_dict = self.parse_aws_log_in_params()
        #     masked_client_options["aws_access_key_id"] = "*****"
        #     masked_client_options["aws_secret_access_key"] = "*****"
        #     # session_token is optional and used only for role based access
        #     if self.aws_log_in_dict.get("aws_session_token", None):
        #         masked_client_options["aws_session_token"] = "*****"
        self.logger.info("Creating postgres client connected to %s with options [%s]", hosts, masked_client_options)

        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        scheme = client_options.get("scheme", "postgres")
        self.client_options["scheme"] = scheme

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options,
                                                                                 "basic_auth_password"):
            self.logger.info("Postgres basic authentication: on")
            self.client_options["userspec"] = \
                f"{self.client_options.pop('basic_auth_user')}:{self.client_options.pop('basic_auth_password')}"
        else:
            self.logger.info("Postgres basic authentication: off")

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        """
        Simplify how Client creation is handled so that it is consistent with ES
        """
        # pylint: disable=import-outside-toplevel
        import psycopg

        return psycopg.connect(host=self.hosts, **self.client_options)

    def create_async(self):
        """
         Postgres uses DBAPI and not HTTP requests, will need to call the on_request_start and
         on_reqeust_end in the runners itself
        :return:
        """

        return RallyAsyncPostgres(hosts=self.hosts,
                                  **self.client_options)

