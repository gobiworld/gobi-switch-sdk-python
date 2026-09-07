# Optionnel: uniquement requis pour IAM / MSK
try:
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    from kafka.oauth.abstract import AbstractTokenProvider
except ImportError:  # pragma: no cover
    MSKAuthTokenProvider = None
    AbstractTokenProvider = object


# ---------------------------------------------------------------------------
# IAM Auth providers
# ---------------------------------------------------------------------------
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka.sasl.oauth import AbstractTokenProvider


class IAMTokenProvider(AbstractTokenProvider):
    """Token provider pour MSK IAM."""

    def __init__(self, region: str):
        if MSKAuthTokenProvider is None:
            raise RuntimeError("aws_msk_iam_sasl_signer non installé")
        self.region = region
        super().__init__()


    def token(self) -> str:
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token