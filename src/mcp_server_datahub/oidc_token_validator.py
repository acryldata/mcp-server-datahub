"""Generic OIDC token validator for OSS deployments.

Provides a standards-compliant OIDC token validator that can work with any
OIDC provider (Auth0, Keycloak, Azure AD, Google, etc.) without vendor-specific
dependencies.

Required environment variables:
- OIDC_ISSUER_URL: The OIDC issuer URL (e.g., https://your-provider.com)
- OIDC_AUDIENCE: Expected audience for token validation

Optional environment variables:
- OIDC_ALGORITHMS: Comma-separated list of allowed algorithms (default: RS256)
- OIDC_LEEWAY: Clock skew tolerance in seconds (default: 10)
"""

import logging
import os
from typing import List, Optional
from urllib.parse import urljoin

import jwt
from jwt import PyJWKClient


logger = logging.getLogger(__name__)


class OIDCTokenValidator:
    """Generic OIDC token validator for any OIDC-compliant provider.

    This validator:
    1. Fetches the provider's JWKS from the well-known endpoint
    2. Validates JWT signatures against the provider's public keys
    3. Validates standard claims (iss, aud, exp, etc.)
    4. Returns the original token (no token exchange)

    This is suitable for DataHub deployments where the OIDC provider
    issues tokens that DataHub GMS can directly accept.
    """

    def __init__(
        self,
        issuer_url: str,
        audience: str,
        algorithms: Optional[List[str]] = None,
        leeway: int = 10,
    ) -> None:
        """Initialize the OIDC token validator.

        Args:
            issuer_url: The OIDC issuer URL
            audience: Expected audience for token validation
            algorithms: List of allowed JWT algorithms (default: ["RS256"])
            leeway: Clock skew tolerance in seconds (default: 10)
        """
        self.issuer_url = issuer_url.rstrip("/")
        self.audience = audience
        self.algorithms = algorithms or ["RS256"]
        self.leeway = leeway

        # Initialize JWKS client
        jwks_uri = urljoin(self.issuer_url + "/", ".well-known/jwks.json")
        self.jwks_client = PyJWKClient(jwks_uri)

        logger.info(
            "Initialized OIDC validator: issuer=%s, audience=%s, algorithms=%s",
            self.issuer_url,
            self.audience,
            self.algorithms,
        )

    def validate_and_resolve(self, token: str) -> str:
        """Validate the OIDC token and return it unchanged.

        Args:
            token: The JWT token to validate

        Returns:
            The same token if validation succeeds

        Raises:
            jwt.InvalidTokenError: If token validation fails
            Exception: For other validation errors
        """
        try:
            # Get the signing key from JWKS
            signing_key = self.jwks_client.get_signing_key_from_jwt(token)

            # Decode and validate the token
            payload = jwt.decode(
                token,
                signing_key.key,
                algorithms=self.algorithms,
                audience=self.audience,
                issuer=self.issuer_url,
                leeway=self.leeway,
            )

            logger.info(
                "Successfully validated OIDC token for subject: %s",
                payload.get("sub", "unknown"),
            )

            return token

        except jwt.InvalidTokenError as e:
            logger.warning("OIDC token validation failed: %s", e)
            raise
        except Exception as e:
            logger.error("Unexpected error during token validation: %s", e)
            raise


def create_oidc_validator() -> Optional[OIDCTokenValidator]:
    """Create an OIDC token validator from environment variables.

    Returns:
        OIDCTokenValidator if properly configured, None otherwise

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    issuer_url = os.getenv("OIDC_ISSUER_URL")
    audience = os.getenv("OIDC_AUDIENCE")

    if not issuer_url:
        logger.info("OIDC token validation disabled (OIDC_ISSUER_URL not set)")
        return None

    if not audience:
        raise ValueError("OIDC_AUDIENCE must be set when OIDC_ISSUER_URL is configured")

    # Parse optional configuration
    algorithms_str = os.getenv("OIDC_ALGORITHMS", "RS256")
    algorithms = [alg.strip() for alg in algorithms_str.split(",") if alg.strip()]

    leeway = int(os.getenv("OIDC_LEEWAY", "10"))

    return OIDCTokenValidator(
        issuer_url=issuer_url,
        audience=audience,
        algorithms=algorithms,
        leeway=leeway,
    )
