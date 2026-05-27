# How to Push to GitHub — Step by Step

## Prerequisites

- Git installed (`git --version`)
- GitHub account
- GitHub CLI optional but recommended (`gh --version`)

---

## Step 1 — Create the GitHub repository

### Option A: GitHub CLI (fastest)
```bash
gh repo create kinesis-pipeline \
  --public \
  --description "Real-time AWS event pipeline: Kinesis → Lambda → Firehose → S3 → Redshift. ~5,000 events/sec, 1.2s latency." \
  --homepage ""
```

### Option B: GitHub web UI
1. Go to https://github.com/new
2. Repository name: `kinesis-pipeline`
3. Description: `Real-time AWS event pipeline: Kinesis → Lambda → Firehose → S3 → Redshift. ~5,000 events/sec, 1.2s latency.`
4. Set to **Public**
5. ⚠️ Do NOT check "Add a README file" — the repo must be empty
6. Click **Create repository**

---

## Step 2 — Connect your local repo and push

```bash
cd kinesis-pipeline

# Add the remote (replace YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/kinesis-pipeline.git

# Push all 12 commits
git push -u origin main
```

You should see output like:
```
Enumerating objects: 94, done.
Counting objects: 100% (94/94), done.
...
 * [new branch]      main -> main
Branch 'main' set up to track remote branch 'main' from 'origin'.
```

---

## Step 3 — Add GitHub Actions secrets (for CI deploy to work)

Go to: **Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|---|---|
| `AWS_ACCESS_KEY_ID` | Your AWS access key |
| `AWS_SECRET_ACCESS_KEY` | Your AWS secret key |

The CI pipeline will fail at the deploy-staging job without these.
Lint, test, and terraform-validate jobs run without any secrets.

---

## Step 4 — Update README badges

Open `README.md` and replace `YOUR_USERNAME` with your actual GitHub username:

```
[![CI](https://github.com/YOUR_USERNAME/kinesis-pipeline/actions/workflows/ci.yml/badge.svg)]
```

Then commit and push:
```bash
git add README.md
git commit -m "docs: update CI badge URL with correct GitHub username"
git push
```

---

## Step 5 — Verify the Actions run

1. Go to https://github.com/YOUR_USERNAME/kinesis-pipeline/actions
2. You should see the CI workflow triggered by your push
3. **lint** and **test** jobs will pass immediately (no AWS needed)
4. **terraform-validate** will pass (no AWS needed)
5. **package** and **deploy-staging** require the AWS secrets from Step 3

---

## Step 6 — Set up branch protection (recommended)

Go to: **Settings → Branches → Add branch protection rule**

- Branch name pattern: `main`
- ✅ Require a pull request before merging
- ✅ Require status checks to pass before merging
  - Add: `lint`, `test`, `terraform-validate`
- ✅ Require branches to be up to date before merging

This ensures the CI must be green before any merge to main.

---

## Step 7 — Add repository topics (improves discoverability)

Go to the repo homepage → click the gear next to **About** → add topics:

```
aws  kinesis  lambda  redshift  firehose  terraform  python  data-engineering
streaming  real-time  etl  serverless
```

---

## What your GitHub repo will show

### Commit history (12 commits)
```
0a57da5  docs: architecture deep-dive covering design decisions and tradeoffs
ad792e0  ci: GitHub Actions pipeline — lint, test, tf-validate, package, deploy
ea3f426  test: 17 unit tests for Lambda handler and Kinesis producer
96e2e14  feat(monitoring): CloudWatch dashboard for pipeline observability
4de3a7b  feat(infra): full Terraform IaC for all pipeline AWS resources
7fe03ff  perf(redshift): batched COPY orchestrator reduces latency 8s → 1.2s
c0d5d96  feat(redshift): events table DDL with DISTKEY/SORTKEY tuning
3deca8d  feat(lambda): idempotent Kinesis transformer with DLQ routing
e3ff1d8  feat(lambda): DynamoDB-backed idempotency store
834f34b  feat(producer): multi-threaded Kinesis PutRecords publisher
6be278e  chore: add dependencies, Makefile, and pytest config
ab81e53  chore: initialize repository scaffold
```

### Commit convention used
| Prefix | Meaning |
|---|---|
| `feat(scope):` | New feature in that component |
| `perf(scope):` | Performance improvement |
| `test:` | Tests only |
| `ci:` | CI/CD pipeline |
| `docs:` | Documentation |
| `chore:` | Tooling, deps, config |

This follows the [Conventional Commits](https://www.conventionalcommits.org/) spec —
standard in open source and used by tools like semantic-release to auto-generate changelogs.

---

## To deploy the actual AWS infrastructure

```bash
# Copy and fill in your values
cp .env.example .env

# Deploy dev environment
make infra-plan ENV=dev     # review what will be created
make infra-apply ENV=dev    # create resources (~3 min)

# Run the producer
source .env
make producer-run

# Create the CloudWatch dashboard
python monitoring/cloudwatch.py --stack kinesis-pipeline-dev
```
