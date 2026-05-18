"""Pluggable token validation interface.

Provides the abstract ``TokenValidator`` protocol that the MCP server uses to
validate and resolve Bearer tokens before creating per-request DataHub clients.

Implementations can validate tokens against any identity provider (OIDC, SAML,
custom JWT, etc.) and optionally exchange them for a DataHub-compatible token.

To create a custom validator, implement the ``TokenValidator`` protocol::

    class MyTokenValidator:
        def validate_and_resolve(self, token: str) -> str:
            # Validate the token, raise on failure.
            # Return a token suitable for DataHub GMS authentication.
            return verified_token

Then register it via the ``TOKEN_VALIDATOR_FACTORY`` environment variable
pointing to a ``module:callable`` that returns a ``TokenValidator`` instance,
or pass it directly to ``_DataHubClientMiddleware``.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class TokenValidator(Protocol):
    """Protocol for token validation and resolution.

    Implementations must provide a ``validate_and_resolve`` method that:
    1. Validates the incoming Bearer token (raises on failure).
    2. Returns a token suitable for DataHub GMS authentication.
       This may be the same token or an exchanged/transformed token.
    """

    def validate_and_resolve(self, token: str) -> str:
        """Validate *token* and return a DataHub-compatible token.

        Parameters
        ----------
        token:
            The raw Bearer token from the HTTP Authorization header.

        Returns
        -------
        str
            A token that DataHub GMS will accept for authentication.

        Raises
        ------
        Exception
            If the token is invalid, expired, or cannot be resolved.
        """
        ...
