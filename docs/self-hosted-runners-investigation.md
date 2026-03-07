# GitHub Actions Self-Hosted Runners Investigation

**Date:** March 7, 2026
**Context:** Botfarm uses ~1800/2000 free GitHub Actions minutes by day 7 of the month. Need to evaluate self-hosted runners as a solution.

---

## TL;DR

**Yes, you can use self-hosted runners on a GitHub Free account right now at zero additional GitHub cost.** The planned $0.002/min platform charge was postponed indefinitely after community backlash. Self-hosted runner minutes currently do NOT count against your 2,000-minute free quota. Your only cost is running your own VM.

However, GitHub has not ruled out charging for self-hosted runners in the future — they said they're "re-evaluating their approach." Plan accordingly.

---

## 1. Current Pricing Status

| Item | Status |
|------|--------|
| Free plan GitHub-hosted minutes | 2,000 min/month (private repos) |
| Self-hosted runner cost to GitHub | **Free** (no per-minute charge) |
| Self-hosted minutes count against quota? | **No** |
| Public repo usage | Always free (hosted or self-hosted) |
| Planned $0.002/min platform charge | **Postponed indefinitely** (was set for March 1, 2026) |
| GitHub-hosted runner price reduction | Active since Jan 1, 2026 (up to 39% cheaper) |

### What happened with the pricing change

- **Dec 16, 2025:** GitHub announced a $0.002/min "cloud platform charge" for self-hosted runners starting March 1, 2026
- **Dec 17, 2025:** Massive community backlash — GitHub walked it back within ~24 hours
- **Current:** GitHub is "taking more time to meet and listen closely to developers, customers, and partners" — no new date announced
- **Risk:** GitHub hasn't cancelled the idea, only postponed it. If/when it returns, self-hosted runner minutes would count against your free 2,000-minute quota

### What the charge would have meant for you

At $0.002/min, if your botfarm runs ~1800 minutes of CI/month, that would be ~$3.60/month. More importantly, those 1800 minutes would consume your free quota, meaning you'd still hit the limit. The real benefit of self-hosted runners is that currently they bypass the quota entirely.

---

## 2. Your Current Workflow

From `.github/workflows/tests.yml`, you run two parallel jobs on every PR:

1. **unit-tests** — Python 3.12, install deps, `pytest tests/ -v -m "not playwright"`
2. **playwright-tests** — Python 3.12, install deps, install Chromium, run e2e tests with screenshots/tracing on failure

Both run on `ubuntu-latest`. The botfarm's automated ticket workflow creates many PRs, which explains the high minute consumption.

---

## 3. GitHub-Hosted Runner Specs (What You're Replacing)

| Spec | Value |
|------|-------|
| vCPUs | 4 |
| RAM | 16 GB |
| Storage | 14 GB SSD |
| CPU models | Intel Xeon Platinum 8370C @ 2.80GHz, AMD EPYC 7763, AMD EPYC 9V74 |
| Single-thread Passmark | ~2,200-2,330 (p50) |
| Max clock speed | ~2.6-2.8 GHz |
| Queue time | ~8-11 seconds before job starts |

**Key insight:** GitHub runners use older, low-clock-speed server CPUs. They're not fast — benchmarks show them scoring poorly on value metrics. A modern consumer/prosumer CPU will easily match or beat them.

---

## 4. VM Requirements to Match or Beat GitHub Runners

### Minimum to match GitHub runners:
- **CPU:** 4 cores, ~2.5+ GHz (any modern x86_64 CPU from the last 5 years)
- **RAM:** 16 GB
- **Storage:** 30+ GB SSD (more headroom than GitHub's 14 GB)
- **Network:** Stable internet for git operations, artifact uploads

### To comfortably beat GitHub runners:
- **CPU:** 4+ cores at 3.5+ GHz (e.g., Intel i5/i7 12th gen+, AMD Ryzen 5/7 5000+)
- **RAM:** 16-32 GB
- **Storage:** NVMe SSD
- **Note:** Even a Raspberry Pi 5 or mini PC would handle your pytest workload. The Playwright tests are more demanding (browser rendering) but still modest.

### Your specific workloads:
- **Unit tests:** CPU-bound, benefits from fast single-thread and parallel cores (pytest-xdist)
- **Playwright tests:** Needs Chromium + display server deps, moderate CPU/RAM
- Both are lightweight — this is a Python test suite, not compiling C++ or building Docker images

**Bottom line:** Almost any modern home server VM with 4 cores and 16 GB RAM will match or exceed GitHub runner performance, likely with faster job start times (no queue delay).

---

## 5. Technical Setup

### Step 1: Prepare the VM

On your home server, create a dedicated VM (or use an existing one) running Ubuntu 22.04/24.04:

```bash
# Create a dedicated user (don't run as root)
sudo useradd -m -s /bin/bash github-runner
sudo passwd github-runner
sudo usermod -aG sudo github-runner

# Install dependencies your workflows need
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3-pip git curl

# For Playwright tests - install browser dependencies
# (these will also be installed by `playwright install chromium --with-deps`)
sudo apt install -y libnss3 libatk-bridge2.0-0 libdrm2 libxcomposite1 \
  libxdamage1 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2
```

### Step 2: Install the GitHub Actions Runner

```bash
su - github-runner
mkdir actions-runner && cd actions-runner

# Download latest runner (check https://github.com/actions/runner/releases for current version)
curl -o actions-runner-linux-x64-2.322.0.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.322.0/actions-runner-linux-x64-2.322.0.tar.gz
tar xzf ./actions-runner-linux-x64-2.322.0.tar.gz
```

### Step 3: Register the Runner with Your Repo

Go to your repo → Settings → Actions → Runners → "New self-hosted runner"

```bash
# Use the token from the GitHub UI
./config.sh --url https://github.com/YOUR_USER/botfarm --token YOUR_TOKEN_HERE

# You'll be asked for:
# - Runner group: default
# - Runner name: e.g., "homeserver-1"
# - Labels: e.g., "self-hosted,linux,x64" (defaults are fine)
# - Work folder: _work (default)
```

### Step 4: Run as a System Service

```bash
# Install and start the service (run from the actions-runner directory)
sudo ./svc.sh install github-runner
sudo ./svc.sh start

# Verify it's running
sudo ./svc.sh status
```

### Step 5: Update Your Workflow

Change `runs-on` from `ubuntu-latest` to `self-hosted`:

```yaml
jobs:
  unit-tests:
    runs-on: self-hosted          # was: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      # Remove setup-python if Python is pre-installed on your runner
      # ...rest of steps
```

### Step 6: (Recommended) Use Labels for Routing

You can use custom labels to route specific jobs:

```yaml
jobs:
  unit-tests:
    runs-on: [self-hosted, linux, x64]
```

---

## 6. Important Differences from GitHub-Hosted Runners

| Aspect | GitHub-Hosted | Self-Hosted |
|--------|--------------|-------------|
| Environment | Fresh VM every job | Persistent (same machine) |
| Cleanup | Automatic | Your responsibility |
| Pre-installed tools | Extensive (~20 GB of tools) | Only what you install |
| Queue time | 8-11 seconds | Near-instant (runner is always on) |
| Concurrent jobs | Limited by plan | Limited by your hardware |
| Maintenance | Zero | You handle updates, security |
| `actions/setup-python` | Downloads/caches Python | Works, but you can pre-install for speed |

### Key considerations:

1. **No automatic cleanup:** Unlike GitHub runners, your environment persists between jobs. Leftover files, installed packages, and state from previous runs can affect future runs. Consider:
   - Using `actions/checkout@v4` with `clean: true`
   - Adding cleanup steps at the end of workflows
   - Or running the runner in a container/VM with snapshots for true isolation

2. **Pre-install dependencies for speed:** Since your environment persists, pre-install Python 3.12, your pip packages, and Playwright browsers. Jobs will be significantly faster since they skip installation steps.

3. **Concurrent jobs:** By default, one runner handles one job at a time. Your workflow has 2 parallel jobs (unit-tests + playwright-tests). Options:
   - Install 2 runner instances on the same VM
   - Run jobs sequentially (simpler but slower)
   - Use the `--maxExecutionJobs` flag if available

---

## 7. Security Considerations

Since this is a **private repository** with only you (and your bots) creating PRs, the security risk is low. Key points:

- **Private repos are safer:** Only people with repo access can trigger workflows. No random PR attack vector.
- **Don't store secrets on the runner machine:** Use GitHub Actions secrets, not files on the VM.
- **Keep the runner updated:** The runner application auto-updates by default.
- **Network isolation:** Your runner has access to your home network. Consider firewall rules to limit what the runner process can reach.
- **If you ever make the repo public:** Remove self-hosted runners immediately. Public repos + self-hosted runners = anyone can run arbitrary code on your machine via PR.

---

## 8. Alternatives Considered

| Alternative | Pros | Cons |
|-------------|------|------|
| **Self-hosted runner (recommended)** | Free, fast, full control | Maintenance overhead |
| **nektos/act (local testing)** | Runs workflows locally | Not a CI replacement, Docker-based, some Actions incompatible |
| **Make repo public** | Unlimited free minutes | Exposes code |
| **Reduce CI runs** | No infra changes | Limits development velocity |
| **Gitea/Forgejo Actions** | Free, compatible syntax | Requires migration off GitHub |
| **CircleCI free tier** | 5 concurrent self-hosted runners | Different workflow syntax, migration effort |
| **Pay for GitHub Pro** | 3,000 min/month | Still might not be enough at your velocity |

---

## 9. Recommendation

**Set up a self-hosted runner on your home server VM.** Here's why:

1. **Immediate relief:** Your 1800 min/month usage drops to 0 counted minutes on GitHub
2. **Better performance:** Your home server CPU likely beats GitHub's 2.8 GHz Xeon chips
3. **Faster jobs:** No queue time + pre-installed deps = significantly faster CI
4. **Zero GitHub cost:** Self-hosted is free today, and the planned charge is postponed
5. **Low effort:** Setup takes ~30 minutes, maintenance is minimal for a single runner
6. **Low risk:** Private repo with controlled access, security concerns are minimal

### Suggested VM allocation:
- **4 vCPUs, 16 GB RAM, 50 GB SSD** (matches GitHub runner specs with headroom)
- Ubuntu 22.04 or 24.04 LTS
- Pre-install: Python 3.12, project dependencies, Playwright + Chromium

### Future-proofing:
- If GitHub eventually charges $0.002/min for self-hosted, your ~1800 min/month would cost ~$3.60/month and consume free quota. At that point, evaluate whether paying $4/month for GitHub Pro (3,000 min) makes more sense than maintaining infrastructure.
- Keep an eye on GitHub's pricing announcements — they'll give advance notice before any changes take effect.

---

## Sources

- [GitHub: Pricing changes for GitHub Actions (2026)](https://github.com/resources/insights/2026-pricing-changes-for-github-actions)
- [GitHub Changelog: Update to GitHub Actions pricing](https://github.blog/changelog/2025-12-16-coming-soon-simpler-pricing-and-a-better-experience-for-github-actions/)
- [GitHub Changelog: Reduced pricing for GitHub-hosted runners](https://github.blog/changelog/2026-01-01-reduced-pricing-for-github-hosted-runners-usage/)
- [GitHub Docs: About billing for GitHub Actions](https://docs.github.com/billing/managing-billing-for-github-actions/about-billing-for-github-actions)
- [GitHub Docs: Self-hosted runners](https://docs.github.com/actions/hosting-your-own-runners)
- [GitHub Docs: GitHub-hosted runners reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
- [The Register: GitHub walks back plan to charge for self-hosted runners](https://www.theregister.com/2025/12/17/github_charge_dev_own_hardware/)
- [Community Discussion: Per minute charges for self hosted runners?](https://github.com/orgs/community/discussions/182089)
- [Community Discussion: Updates to GitHub Actions pricing](https://github.com/orgs/community/discussions/182186)
- [RunsOn: GitHub Actions CPU Performance Benchmarks](https://runs-on.com/benchmarks/github-actions-cpu-performance/)
- [Northflank: GitHub self-hosted runners cost increase and alternatives](https://northflank.com/blog/github-pricing-change-self-hosted-alternatives-github-actions)
- [DevOps Cube: Setup GitHub Actions Self-Hosted Runner](https://devopscube.com/github-actions-self-hosted-runner/)
- [Blacksmith: The GitHub Actions control plane is no longer free](https://www.blacksmith.sh/blog/actions-pricing)
