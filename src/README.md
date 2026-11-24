# payments-api

`POST /auth/token` accepts JSON containing `username` (the register's `ecr_id`)
and `password`. Enabled registers with valid Argon2id credentials receive:

```json
{"access_token":"<signed JWT>","token_type":"Bearer","expires_in":900}
```

The response includes `Cache-Control: no-store`. Unknown registers, incorrect
passwords and disabled registers all receive `401 {"detail":"Invalid credentials"}`.
Invalid request bodies receive a generic 422 without submitted values. Database,
configuration, ADC and KMS failures receive a generic 503 with no token. This
endpoint does not log request bodies, credentials, tokens or dependency exceptions.
Keep request/response body capture disabled for this route in any external logging
or tracing integration as well.

**No shared login rate limiter exists in this repository. Add shared rate limiting
before exposing this endpoint publicly**, including across Cloud Run instances.

## Assumed database schema

The repository does not define `ecr_clients`. The endpoint assumes this minimum
schema in the existing PostgreSQL database's search path; compare it with your
existing table. No migration or database change is applied by this implementation.

```sql
CREATE TABLE ecr_clients (
    ecr_id TEXT PRIMARY KEY,
    credential_hash TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);
```

`ecr_id` must be unique and returned as a string. `credential_hash` must contain the
**entire encoded Argon2id hash**, including `$argon2id$`, version, parameters, salt
and hash. Truncated hashes, plaintext and other hash algorithms fail authentication.
Passwords are verified exactly as submitted, without trimming or normalization.
The lookup binds `username` as a SQL parameter using the existing `app.db.get_conn`.

## Configuration

All four new environment variables are required. Configuration and the ADC KMS
client are cached per process; restart the service after changing configuration.

| Variable | Value |
| --- | --- |
| `ECR_JWT_KMS_KEY_VERSION` | Full KMS key version resource name; dev value below |
| `ECR_JWT_ISSUER` | The issuer expected by your JWT consumers |
| `ECR_JWT_AUDIENCE` | The audience expected by your JWT consumers |
| `ECR_JWT_KEY_ID` | JWT header `kid` corresponding to this key version |

Dev key version:

```text
projects/pos-dev-485421/locations/us-central1/keyRings/payments-auth/cryptoKeys/ecr-jwt/cryptoKeyVersions/1
```

Use an enabled `ASYMMETRIC_SIGN` key version with algorithm
`RSA_SIGN_PKCS1_2048_SHA256`, `RSA_SIGN_PKCS1_3072_SHA256` or
`RSA_SIGN_PKCS1_4096_SHA256` for RS256. The Cloud Run runtime service account needs
`cloudkms.cryptoKeyVersions.useToSign` on the key (for example through
`roles/cloudkms.signerVerifier`). The client uses Application Default Credentials;
there is no service account key file or credential override in the implementation.
See Google's [KMS signing documentation](https://docs.cloud.google.com/kms/docs/create-validate-signatures)
and [algorithm reference](https://docs.cloud.google.com/kms/docs/algorithms).

The implementation sends the SHA-256 digest of the compact JWS signing input to
`asymmetric_sign`, with CRC32C integrity checks and a 10-second RPC timeout without
automatic retries. Claims are `sub` = database `ecr_id`, configured `iss` and `aud`,
UTC Unix-second `iat`, and `exp` = `iat + 900`.

Existing database variables are still required: `INSTANCE_CONNECTION_NAME`,
`DB_USER`, `DB_PASS` and `DB_NAME`. The existing Cloud SQL connector uses public IP;
local testing requires connectivity and ADC permissions for that instance.

## Local setup and tests

From the repository root, use Python 3.11 to match `src/Dockerfile`:

```sh
python3.11 -m venv /tmp/payments-api-venv
source /tmp/payments-api-venv/bin/activate
python -m pip install -r src/requirements-dev.txt
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m pytest -q -p no:cacheprovider src/tests
```

Tests mock the database, KMS and the existing import-time Pub/Sub publisher. No
live credentials, environment setup or cloud services are needed. The success
test verifies the JWT signature with a test RSA public key and checks the claims
and 900-second lifetime. Failure tests cover credentials, validation, database,
configuration, ADC, KMS errors and KMS integrity failures.

To run against your dev database and KMS, configure local ADC:

```sh
gcloud auth application-default login
export ECR_JWT_KMS_KEY_VERSION='projects/pos-dev-485421/locations/us-central1/keyRings/payments-auth/cryptoKeys/ecr-jwt/cryptoKeyVersions/1'
export ECR_JWT_ISSUER='https://your-dev-issuer.example'
export ECR_JWT_AUDIENCE='your-dev-audience'
export ECR_JWT_KEY_ID='your-dev-key-id'
# Set INSTANCE_CONNECTION_NAME, DB_USER, DB_PASS and DB_NAME securely as usual.
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m uvicorn app.main:app --host 127.0.0.1 --port 8080
```

Your local ADC identity must have the same required database and signing access.
In a second terminal, submit credentials interactively so the password does not
appear in shell history or process arguments. This verifies receipt without
printing the bearer token:

```sh
python3 -c '
import getpass
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

body = json.dumps({
    "username": input("ECR ID: "),
    "password": getpass.getpass("Password: "),
}).encode()
request = Request(
    "http://127.0.0.1:8080/auth/token",
    data=body,
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urlopen(request) as response:
        result = json.load(response)
        print(response.status, response.headers.get("Cache-Control"))
        print(result["token_type"], result["expires_in"], "token received:", bool(result["access_token"]))
except HTTPError as error:
    print(error.code, error.read().decode())
'
```

## ECR bearer authentication

Every payment operation requires an ECR JWT through the shared
`app.ecr_auth.require_ecr_jwt` FastAPI dependency:

| Method | Path |
| --- | --- |
| GET | `/payments/{payment_id}` |
| POST | `/payments/pay` |
| POST | `/payments/gift` |
| POST | `/payments/{payment_id}/events` |
| POST | `/payments/{payment_id}/cancel` |
| POST | `/payments/{payment_id}/void` |
| POST | `/payments/{payment_id}/return` |
| POST | `/terminals/{terminal_id}/batch-sync` |

`GET /health`, `POST /auth/token` and `GET /.well-known/jwks.json` are explicitly
public in the checked-in Gateway OpenAPI specification and have no ECR JWT
dependency. Token issuance still requires valid register credentials. JWKS returns
only the configured key's public RSA modulus, exponent, algorithm and key ID.
The existing FastAPI documentation URLs are not Gateway operations and retain
their existing access behavior.

The events and batch-sync operations appear to be callbacks from a terminal or
agent. They require an ECR JWT under this policy too. Confirm that these callers
can obtain and send one; no alternate caller authentication is introduced here.

The existing top-level `x-google-backend.address` enables Gateway backend identity
authentication by default, with the Cloud Run address as the backend audience.
Gateway puts its service account's identity token in `Authorization` and copies
the client's original header to `X-Forwarded-Authorization`. The dependency prefers
that forwarded header when present, and supports `Authorization: Bearer <ECR JWT>`
for direct/local requests. Empty, malformed or duplicate forwarded credentials
are rejected without falling back to another token. See Google's
[backend authentication documentation](https://docs.cloud.google.com/api-gateway/docs/oasv2-extensions).

All headers are untrusted until the ECR JWT signature is verified. The dependency
does not trust `X-Apigateway-Api-Userinfo`, does not use token-supplied signing keys
or URLs, and never logs bearer tokens. It requires RS256, the configured `kid`,
issuer and audience, an unexpired `exp`, and a nonempty string `sub`. Invalid or
missing credentials return `401 {"detail":"Invalid authentication credentials"}`
with `WWW-Authenticate: Bearer`, before the payment handler executes. The verified
subject is available as `request.state.ecr_id`. No merchant, store or terminal
authorization rules are applied.

The public verification key is fetched using ADC from `ECR_JWT_KMS_KEY_VERSION`,
checked for the configured KMS version, RS256-compatible algorithm and CRC32C,
and cached per process. The runtime service account therefore also needs
`cloudkms.cryptoKeyVersions.viewPublicKey` (for example through
`roles/cloudkms.publicKeyViewer`); see Google's
[public key permissions](https://docs.cloud.google.com/kms/docs/retrieve-public-key).
Key retrieval failures fail authentication closed with the same generic 401.
The JWKS endpoint returns a generic 503 if its key cannot be retrieved. Only the
current configured key version is accepted/published; restart after changing it.

The Gateway definition uses `payments-api-dev` for issuer and audience, matching
the existing Cloud Build environment variables. Its trusted JWKS URL is
`https://payments-api-207519926302.us-central1.run.app/.well-known/jwks.json`, based
on the repository's existing backend URL. Before deploying, verify:

- That backend address and JWKS URL identify the intended Cloud Run service, and
  that JWKS is reachable by Gateway's public-key fetcher without an ECR token.
- Cloud Run's four `ECR_JWT_*` variables and the Gateway issuer/audience definition
  match, and that `kid` identifies the configured KMS public key.
- The runtime service account has KMS signing and public-key retrieval permission.
- The deployed Gateway API config uses this OpenAPI file and the expected backend
  service account, with `roles/run.invoker` if Cloud Run IAM requires it.
- Cloud Run ingress/IAM has the intended direct-access policy. The existing
  pipeline uses `--allow-unauthenticated`; application JWT verification still
  protects the payment routes even if the service is directly reachable.

No deployment settings are changed by the authentication implementation. Gateway
API configuration publication and deployment remain separate actions.

Existing request-schema differences remain for comparison with the Gateway file:

- `/payments/pay`: Gateway declares `invoice_id` required and `amount` as a number;
  FastAPI allows an omitted invoice and requires a positive integer amount in cents.
- `/terminals/{terminal_id}/batch-sync`: FastAPI also requires `source`.
- `/payments/{payment_id}/events`: FastAPI also requires `occurred_at`; the Gateway
  schema documents `message`/`meta`, which are not model fields, and omits several
  processor fields present in the FastAPI model.

The route paths match. These existing request schemas and all payment handler
bodies remain unchanged. Authentication tests compare every Gateway operation's
security policy with its FastAPI dependency, reject invalid tokens before database
or payment calls, exercise the existing handlers with valid tokens, check request
context and Gateway header precedence, and confirm the public endpoints work
without an ECR token. All keys and cloud calls are mocked in tests.
