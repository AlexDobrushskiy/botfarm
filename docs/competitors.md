# Competitive Landscape — Ticket-to-PR Systems

> Last updated: 2026-02-24

## How They Work

Every competing ticket-to-PR product is SaaS. They run model inference on their own infrastructure and bundle it into their pricing. Users never provide model API keys — they pay the vendor, and the vendor handles all compute.

Users *do* share service credentials (GitHub tokens, Linear/Jira API keys) so the agent can access repos and issue trackers. But model access is entirely vendor-side.

---

## Billing Models

### 1. Bundled Compute Units — Devin (Cognition)

- **Pricing:** $20/mo Core, $500/mo Teams, custom Enterprise
- **Unit:** Agent Compute Units (ACUs) — ~15 min of agent work per ACU at $2–2.25 each
- **How it works:** Devin runs a sandboxed VM with full browser + shell + IDE. Model inference is bundled — you never see an API key or choose a model. You provide GitHub/cloud service credentials so Devin can access your repos.
- **Deployment:** Pure SaaS. Enterprise tier offers VPC option for data isolation.

### 2. Token-Based Usage — Factory AI, Codegen

**Factory AI:**
- **Pricing:** Starts at $20/mo, $200/mo Max tier (200M Standard Tokens)
- **How it works:** Per-team subscription + token consumption. Factory is model-agnostic (OpenAI, Anthropic, Cohere) but *they* hold the API keys and call the models on your behalf. Specialized "Droids" (Knowledge Droid, Code Droid) handle different stages. Deep Jira/Linear/Slack integration.
- **Deployment:** SaaS only.

**Codegen:**
- **Pricing:** Free trial, paid tiers (not publicly detailed)
- **How it works:** SaaS platform. Interact via Slack, Linear, GitHub, or web UI. Python SDK for programmatic control. SOC 2 Type II certified. They handle all model inference.
- **Deployment:** SaaS only.

### 3. Platform Subscription — GitHub Copilot Coding Agent

- **Pricing:** Included with Copilot Pro ($10/mo), Business ($19/user/mo), Enterprise ($39/user/mo)
- **How it works:** Assign a task via `@copilot` on a GitHub issue. Copilot runs autonomously in GitHub Actions, creates a draft PR. Multi-model support — Claude Sonnet 4.6, Claude Opus 4.6, GPT-4.1, etc. Premium models consume "premium requests" from monthly allowance; overage at $0.04/request on Business.
- **Deployment:** GitHub cloud only.

### 4. IDE Plugin — Sweep AI

- **Pricing:** Free trial, Basic $10/mo, Pro $20/mo, Ultra $60/mo
- **How it works:** Originally a GitHub App for issue-to-PR automation, now primarily a JetBrains IDE plugin. Describe a task in a GitHub issue or Jira ticket, Sweep reads your project, writes code, creates a PR. API credits consumed for AI features; autocomplete unlimited on paid plans.
- **Deployment:** SaaS + IDE plugin.

---

## Summary Comparison

| | Devin | Factory | Codegen | GitHub Copilot Agent | Sweep | **Botfarm** |
|---|---|---|---|---|---|---|
| **Type** | SaaS | SaaS | SaaS | SaaS (GitHub) | SaaS + IDE | **Self-hosted** |
| **Model access** | Bundled | Bundled | Bundled | Bundled | Bundled | **Your Claude Max sub** |
| **Bring your own key** | No | No | No | No | No | **Yes (implicitly)** |
| **Code leaves your machine** | Yes | Yes | Yes | Yes | Yes | **No** |
| **Model choice** | Vendor-selected | Multi-model | Vendor-selected | Multi-model picker | Vendor-selected | **Claude only** |
| **Base cost** | $20+/mo | $20+/mo | Free trial | $10+/mo | $10+/mo | **$0 (+ Claude Max)** |
| **Per-task cost** | ~$2.25/ACU | Token-based | Token-based | Premium requests | Credit-based | **$0 on Max plan** |
| **Issue tracker** | GitHub, Slack | Jira, Linear, Slack | Jira, Linear, GitHub | GitHub Issues | GitHub, Jira | **Linear** |
| **Self-hosting** | No (VPC on Enterprise) | No | No | No | No | **Yes (only option)** |
| **Open source** | No | No | No | No | No | **Yes** |

---

## Where Botfarm Fits

Botfarm is the only tool that **leverages an existing Claude Code subscription directly**. It wraps `claude -p` as a subprocess on your machine.

**Advantages over SaaS competitors:**
- Zero additional model cost on Claude Max ($100–200/mo unlimited)
- No API keys shared with any third party
- Code never leaves your machine
- Full control over pipeline stages, retry logic, and configuration
- No vendor lock-in — you own the orchestration layer

**Trade-offs:**
- Claude models only (no multi-model)
- You run the infrastructure (machine running 24/7)
- Linear only (no Jira/GitHub Issues — yet)
- Single-user (no team features, RBAC, or shared dashboards)

---

## Sources

- [Devin Pricing](https://devin.ai/pricing/)
- [Devin Pricing Breakdown — Lindy](https://www.lindy.ai/blog/devin-pricing)
- [Factory AI Pricing](https://factory.ai/pricing)
- [Factory Pricing Docs](https://docs.factory.ai/pricing)
- [Codegen](https://codegen.com/pricing)
- [GitHub Copilot Plans](https://github.com/features/copilot/plans)
- [GitHub Copilot Coding Agent Model Picker](https://github.blog/changelog/2026-02-19-model-picker-for-copilot-coding-agent-for-copilot-business-and-enterprise-users/)
- [Sweep AI Pricing](https://sweep.dev/pricing)
