# Getting Started

Single page quick start: prerequisites, environment variables, validation, usage, troubleshooting & security.

## 1. Install

```bash
pip install dsis-client
```

## 2. Prerequisites

You need:

1. Azure AD app registration (client id + secret).
2. Consent/access to the DSIS access application.
3. DSIS username & password for target environment.
4. Subscription key for the Enterprise product in APIM portal.

Treat all identifiers and secrets as confidential; never store them in source control.

## 3. Environment Variables

Define (placeholder names only):

```text
DSIS_TENANT_ID=<tenant>
DSIS_CLIENT_ID=<client-id>
DSIS_CLIENT_SECRET=<secret>
DSIS_ACCESS_APP_ID=<access-app-id>
DSIS_USERNAME=<dsis-user>
DSIS_PASSWORD=<dsis-password>
DSIS_SUBSCRIPTION_KEY_DSAUTH=<subscription-key-for-dsauth>
DSIS_SUBSCRIPTION_KEY_DSDATA=<subscription-key-for-dsdata>
```

Load via your platform (shell export, CI variables, secret manager). Avoid committing real values.

### Optional Validation Snippet

```python
import os
required = [
    "DSIS_TENANT_ID", "DSIS_CLIENT_ID", "DSIS_CLIENT_SECRET",
    "DSIS_ACCESS_APP_ID", "DSIS_USERNAME", "DSIS_PASSWORD",
    "DSIS_SUBSCRIPTION_KEY_DSAUTH", "DSIS_SUBSCRIPTION_KEY_DSDATA"
]
missing = [v for v in required if not os.getenv(v)]
if missing:
    raise SystemExit(f"Missing variables: {', '.join(missing)}")
```

## 4. Configuration & Usage

```python
from dsis_client import DSISClient, DSISConfig, Environment
import os

config = DSISConfig(
    environment=Environment.PROD,
    tenant_id=os.getenv("DSIS_TENANT_ID"),
    client_id=os.getenv("DSIS_CLIENT_ID"),
    client_secret=os.getenv("DSIS_CLIENT_SECRET"),
    access_app_id=os.getenv("DSIS_ACCESS_APP_ID"),
    dsis_username=os.getenv("DSIS_USERNAME"),
    dsis_password=os.getenv("DSIS_PASSWORD"),
    subscription_key_dsauth=os.getenv("DSIS_SUBSCRIPTION_KEY_DSAUTH"),
    subscription_key_dsdata=os.getenv("DSIS_SUBSCRIPTION_KEY_DSDATA")
)

client = DSISClient(config)
if client.test_connection():
    # Get data - schema refers to data schemas like "Well", "Basin", "Fault"
    data = client.get(district_id="<district-id>", project="<project-name>", schema="Basin")
```

## 5. Flow Summary

1. Acquire Azure AD token (internal)
2. Exchange for DSIS token
3. Perform data calls with both tokens + subscription key

## 6. Common Issues

| Symptom | Likely Cause | Action |
|---------|--------------|--------|
| 401 Unauthorized | Wrong / expired secret or missing consent | Re-issue secret, confirm consent |
| 403 Forbidden | Access app id or subscription key mismatch | Verify values & product subscription |
| 404 / empty | Endpoint/table/record not present | Check spelling / availability |
| Timeout | Network / proxy restrictions | Test connectivity, configure proxy |

## 7. Security Reminders

- Never print secrets.
- Rotate secrets & subscription keys.
- Separate credentials per environment.
- Prefer managed secret stores (e.g. Key Vault) over flat files.

## 8. Next

See `api/index.md` for method & configuration summary.
