# Cloudflare setup for the SpicyRegs fork

The `mikewolfd/spicy-regs` fork uses **Mdeeb@civictechdc.org's Account**,
`174055408ff1560e60601c4d12c561c4`. Its data bucket, optional catalog, deployment
credentials and Terraform state must all belong to that account.

## Account selection

The Cloudflare Worker configuration pins the account ID. Local Wrangler uses the
named `spicy-regs-civictechdc` login; directory bindings are local machine
settings and do not travel with Git. A new checkout can establish them with:

```sh
cd deploy/cloudflare
npm ci
npx wrangler auth create spicy-regs-civictechdc
npx wrangler auth activate spicy-regs-civictechdc ../..
npx wrangler whoami
```

Verify the login email and account ID above before provisioning. Wrangler
4.136.1 or later supports the named login used here. These bindings leave other
projects' logins unchanged. Account IDs are identifiers, not API credentials.

## Public domain

The fork uses the GitHub Actions repository variable `SPICYREGS_DOMAIN=spicygov.ai`.
See [public domain configuration](public-domain.md) for local deployment commands,
URL override precedence and the separate DNS/Pages settings.

## Storage and GitHub Actions

The `spicy-regs` data bucket is provisioned in this account. Its initial public
URL is `https://pub-72e95c0c20a84508b42b03a6ff6d55f8.r2.dev`, kept enabled. Production reads use the custom domain
`https://data.spicygov.ai` (attached 2026-09-29, zone `spicygov.ai`, no edge caching of Parquet: `.parquet` is not a cached
extension and mutable keys carry `no-cache, must-revalidate`; don't enable "Cache Everything" on the zone).
Browser reads allow `GET` and `HEAD` from any origin, including byte-range reads;
the applied settings are in `cloudflare/r2-cors.json`. Use a custom domain for production traffic
because the development hostname is rate limited. See Cloudflare's
[public bucket guidance](https://developers.cloudflare.com/r2/buckets/public-buckets/).

The existing workflows read the following **repository secrets** from
`mikewolfd/spicy-regs`:

| Setting | Value or purpose |
| --- | --- |
| `R2_ENDPOINT` | `https://174055408ff1560e60601c4d12c561c4.r2.cloudflarestorage.com` |
| `R2_BUCKET_NAME` | `spicy-regs` |
| `R2_PUBLIC_URL` | Optional explicit override: `https://data.spicygov.ai` |
| `R2_ACCESS_KEY_ID` | S3 access key scoped to the data bucket |
| `R2_SECRET_ACCESS_KEY` | Matching S3 secret, supplied through a secure prompt |

`CLOUDFLARE_ACCOUNT_ID` is also recorded as a repository variable for deployment
tools. Set the repository variable `PUBLISH_DOCKET_SEARCH` to `true` only where
the Spicy Regs web app reads this bucket: the regulatory refresh then builds the
app's `docket_search.json.gz`. The fork leaves it unset because its app
deployment reads upstream's copy; upstream must set it before adopting this
workflow. GitHub does not copy upstream secrets into a fork. Keep the five storage
settings consistent: readers use the public URL while writers use the S3 endpoint
and bucket. The [R2 authentication guide](https://developers.cloudflare.com/r2/api/tokens/)
describes the separate S3 credentials; a Wrangler OAuth login is not an S3 key.

Use `gh secret set NAME --repo mikewolfd/spicy-regs` to enter a secret securely.
Do not put credential values in command arguments, tracked files, or chat.
Local runs use the same settings in the ignored repository `.env` file.

CLI, analytics, MCP and freshness checks choose `SPICY_REGS_R2_URL`, then
`R2_PUBLIC_URL`, then `https://data.${SPICYREGS_DOMAIN}` (upstream domain when
unset). Actions derive the data URL unless the existing secret overrides it. A
read-only override does not change where a pipeline uploads data.

## Catalog and optional cache purge

The ETL needs the R2 Data Catalog on this account's bucket: every workflow run
uses Iceberg, and comments ingest only through the catalog.
Set `R2_CATALOG_URI`, `R2_CATALOG_WAREHOUSE`, `R2_CATALOG_TOKEN` and optionally
`R2_CATALOG_NAMESPACE` in the fork secrets and local environment. Copy the URI
and warehouse from the actual catalog; do not reuse an upstream catalog token.
The first ETL write initializes native subject and receipt tables together.
Use `--allow-fresh-start` for a deliberate first acquisition. Existing loose
Parquet tables do not initialize the catalog; readers require its recorded
initialization and validated receipts.

Cache purge is optional. Set `CLOUDFLARE_ZONE_ID` and `CLOUDFLARE_API_TOKEN` only
for the zone serving this bucket, using a token scoped to cache purge. This token
is separate from the local Wrangler login and deployment token. R2 development
hostnames do not use a zone owned by this fork.

## MCP image registry credential

The manual reader-image workflow uses the dedicated repository secret
`CLOUDFLARE_CONTAINER_REGISTRY_TOKEN`. Create an API token with the account
**Containers Write** permission (shown as **Containers Edit** in some token
interfaces), restricted to account `174055408ff1560e60601c4d12c561c4`. Cloudflare
lists this under [account token permissions](https://developers.cloudflare.com/fundamentals/api/reference/permissions/#account-permissions).
The workflow requests temporary `push` and `pull` credentials through the
[registry credentials API](https://developers.cloudflare.com/api/resources/containers/subresources/registries/subresources/credentials/methods/generate/).
It maps the dedicated secret to Wrangler's `CLOUDFLARE_API_TOKEN` only for that
authentication step. Keep the existing zone cache-purge token unchanged.

Set the dedicated secret through GitHub's secret UI or the secure input of
`gh secret set CLOUDFLARE_CONTAINER_REGISTRY_TOKEN --repo mikewolfd/spicy-regs`.
Keep `CLOUDFLARE_ACCOUNT_ID` equal to the account pinned in the Worker template.
Do not copy a local OAuth login into GitHub. The token's presence alone does not
prove registry access: the workflow's credential request and Docker login must
succeed before the image build starts. Missing or insufficient credentials
leave the required image build pending; no credential change or deployment is
performed by this workflow. See the [image release procedure](cloudflare/README.md#qualified-fec-views).

## MCP hosting

Export `SPICYREGS_DOMAIN=spicygov.ai` and
`MCP_GUIDE_URL=https://spicygov.ai/mcp/` before running the Cloudflare npm commands.
They render `wrangler.jsonc` from `wrangler.template.jsonc`, deriving the MCP route
and `vars.SPICY_REGS_R2_URL` from the domain. The Worker returns 503 if that setting is cleared.
For Iceberg, fill the three catalog variables in the template and install the Worker secret
with `npx wrangler secret put R2_CATALOG_TOKEN`. The Worker explicitly forwards
these settings and the optional secret to its Python container.

Run `npm run check` before deployment. This generates types, checks TypeScript,
and builds the container locally. It does not deploy it or populate the bucket.

The fork's server is live at `https://mcp.spicygov.ai/mcp` (custom domain since 2026-09-29) and
`https://spicy-regs-mcp.mdeeb.workers.dev/mcp`
(first deployed 2026-09-25 from `f2df979`, redeployed 2026-09-27 from `2963d77` as version
`925124f0…`; the first deploy came after Containers was enabled on the account; the first attempt uploaded only the Worker because the Containers API
answered 401). A remote MCP client adds that URL as a connector. The same smoke
script was run against the exact image locally and against the live endpoint:
the three tools answer, `describe_table` names each table's managed generation,
and five cross-source joins return rows (FEC candidate to legislator and votes,
laws to Table III, compiled hearing dates, organization to FEC committee,
USAspending to SAM by UEI). That early reader read each table's footer during
cold discovery. Its script and receipts are in
`~/Work/corpora/fork-execution-2026-09-21/r7-mcp-2026-09-25/`.

The maintained [Worker template](cloudflare/wrangler.template.jsonc) pins the
reader built from `67ef430e57b2ab55f09a66faaa9008e3bcfe8dc3` and its matching
Federal Election Commission compatibility receipt. Keep the registry image and
both runtime pins together when updating the template. Confirm the running
container and live MCP response after deployment.
[Reader image build and association](https://github.com/mikewolfd/spicy-regs/actions/runs/42077305450)

On November 5, 2026, Cloudflare reported Worker version
`8a25d4a5-8ba1-4bd9-9c41-116cac029969` and container app version `64`.
The public MCP check verified discovery, table descriptions, and representative
lobbying and campaign-finance queries. The public Explorer metadata matched the
selected lobbying generation. The browser opened both filing references and
reopened the same client from its deep link.
[Release CI](https://github.com/mikewolfd/spicy-regs/actions/runs/42077204001)

The Cloud Run script describes the historical upstream service. A fork using
that separate hosting path needs its own project, data URL, catalog settings and
smoke-test target; the existing script is not a fork deployment recipe.

## Terraform state

`deploy/terraform/backend.tf` contains no account endpoint. Copy
`backend.hcl.example` to the ignored `backend.hcl`, filling this account's endpoint
and a **separate private state bucket**, then initialize with
`terraform init -backend-config=backend.hcl`. Use fresh local state for this fork;
do not migrate upstream state into it.

The default configuration has no active import blocks. Copy `imports.tf.example`
only to adopt resources that already exist in the selected account, replacing the
ruleset ID with its real value. Set the account, zone, custom domain and allowed
browser origins explicitly in `terraform.tfvars`. Review a saved plan before
applying: the cache resource manages the zone's cache-settings ruleset, so an
existing zone may have other rules that must be preserved.

## Validate before the first data run

Verify bucket ownership, scoped S3 read/write access, the public object's bytes,
and the selected URL in CLI/MCP reads. Then run one bounded publication check,
including its conditional generation update. Account setup does not migrate
historical data or establish table coverage. Keep acquisition keys and the scope
of the first backfill explicit.

### Verified setup, 2026-09-21

The named login, bucket, public URL, CORS and five fork storage secrets are
configured. A synthetic two-table family passed the real S3 publication path:
Rulespec admission, exact public byte checks, repeat publication and refusal of
a stale conditional pointer update. A 6 MiB multipart upload also survived a
repeat attempt with identical bytes. Browser preflight allows `Range` and
`If-Match`. All ten rehearsal objects were deleted; no unfinished multipart
uploads remained. Receipts are retained outside Git in
`~/Work/corpora/fork-cloudflare-2026-09-21/`.

This verifies storage setup, not source data coverage. After the keys were
installed, the existing scheduled `lobbying_filings` job published a generation
with zero rows (422 bytes, 14 columns) after the source rejected an unfiltered
request with HTTP 400. Its public bytes and decoded row count match the
publication record, but the source failure did not establish absence. The
invalid family was conditionally withdrawn from `publication.json` (the v1 pointer, no longer written); its
immutable files and receipts remain available for diagnosis.
The reader repair in `7551f63` refuses failed, malformed and incomplete pages
before output or publication. Independent replay verifies both former
false-empty and false-partial failures; 63 focused tests and the full 2,107-test
host gate pass. Two live pages yield three records whose 42 output cells match
the native source. This qualifies the repair and sample, not a full backfill.
The source now requires a filter for pagination. The scope-preserving date
filter reports 1,977,046 filings, so a keyless cold start would require about
79,082 pages. The lobbying schedule is paused pending a bounded initial backfill;
successful storage and a small source sample do not establish that backfill.
The catalog, custom domain and Worker deployment remain separate steps.
Catalog-backed ingestion and mirror jobs need the catalog settings above.

A later [full generation inventory](../docs/fork-generation.md) found a separate
invalid active SAM family: its run lacked a source key but published a zero-row,
575-byte file. That family has not been withdrawn. Only the lobbying schedule
was paused; storage setup does not make the other schedules ready. The fork now
has `DATA_GOV_API_KEY` (one-record OpenFEC check passed) and `ZYTE_TOKEN` (not yet
wired), while full per-source acquisitions remain separately qualified.
The [local reuse inventory](../docs/research/local-data-reuse-2026-09-21.md)
identifies existing sealed outputs and source data available for the initial
generation campaign.

Core GitHub CI passed for the setup commit. The separate documentation build
passed, but deployment returned 404 because GitHub Pages is not enabled on this
fork.

The cross-repository [remaining-gaps register](../../spicy-docs/docs/research/remaining-gaps-2026-09-21.md)
tracks source coverage, corrected-data adoption, catalog setup and optional
serving paths separately. The [failed Pages run](https://github.com/mikewolfd/spicy-regs/actions/runs/35642543802)
and retained `pages-deployment-failure.log` preserve the deployment evidence.
