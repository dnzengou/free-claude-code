# Free Claude Code — Project Blueprint
**Version:** 2.0.0 · **Updated:** 2026-05-29 · **Python:** 3.14.0

---

## Executive Summary

Drop-in Anthropic Messages API proxy that routes Claude Code CLI traffic to 17+ provider backends. Keeps Claude Code's client-side protocol stable while swapping the underlying model. Ships with a local Admin UI, Discord/Telegram bot wrapper, and optional voice-note transcription.

---

## Architecture

```
Claude Code CLI / VS Code / JetBrains ACP
        │  Anthropic Messages API
        ▼
┌─────────────────────────────────────────┐
│           FastAPI Proxy (server.py)     │
│  ┌──────────┐  ┌──────────────────────┐ │
│  │  /admin  │  │  /v1/messages        │ │
│  │  Admin   │  │  /v1/models          │ │
│  │  UI      │  │  /v1/messages/count  │ │
│  └──────────┘  └──────────┬───────────┘ │
│                           │             │
│  ┌────────────────────────▼───────────┐ │
│  │         Model Router               │ │
│  │  Opus → MODEL_OPUS                 │ │
│  │  Sonnet → MODEL_SONNET             │ │
│  │  Haiku → MODEL_HAIKU               │ │
│  │  * → MODEL (fallback)              │ │
│  └────────────────────────┬───────────┘ │
└───────────────────────────┼─────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
 AnthropicMessages    OpenAICompat         Local
 Transport            Transport            Providers
 (DeepSeek, Kimi,    (NIM, OpenRouter,    (LM Studio,
  Wafer, Fireworks,   Gemini, Groq,        llama.cpp,
  Z.ai)              Cerebras, Mistral,    Ollama)
                      OpenCode, Codestral)
```

---

## Module Map

| Package | Responsibility |
|---|---|
| `server.py` | ASGI entry point — `create_asgi_app()` |
| `api/` | FastAPI routes, admin UI, services, model routing, request optimizations |
| `core/anthropic/` | Shared Anthropic protocol: SSE, content, conversion, thinking, tokens, tools |
| `core/` | Rate limiting, trace/logging utilities |
| `providers/` | Provider transports (17 backends), registry, base classes, error mapping |
| `config/` | Settings (`pydantic-settings`), provider catalog, logging config, paths |
| `messaging/` | Discord/Telegram platforms, session trees, voice transcription |
| `cli/` | `fcc-server`, `fcc-claude`, `fcc-init` entry points; process registry |
| `tests/` | 1429 unit, contract, and integration tests |

---

## Provider Registry

| ID | Transport | Auth |
|---|---|---|
| `nvidia_nim` | OpenAI compat | `NVIDIA_NIM_API_KEY` |
| `open_router` | OpenAI compat | `OPENROUTER_API_KEY` |
| `gemini` | OpenAI compat | `GEMINI_API_KEY` |
| `mistral` | OpenAI compat | `MISTRAL_API_KEY` |
| `mistral_codestral` | OpenAI compat | `CODESTRAL_API_KEY` |
| `opencode` | OpenAI compat | `OPENCODE_API_KEY` |
| `opencode_go` | OpenAI compat | `OPENCODE_API_KEY` |
| `groq` | OpenAI compat | `GROQ_API_KEY` |
| `cerebras` | OpenAI compat | `CEREBRAS_API_KEY` |
| `deepseek` | Anthropic native | `DEEPSEEK_API_KEY` |
| `kimi` | Anthropic native | `KIMI_API_KEY` |
| `wafer` | Anthropic native | `WAFER_API_KEY` |
| `fireworks` | Anthropic native | `FIREWORKS_API_KEY` |
| `zai` | Anthropic native | `ZAI_API_KEY` |
| `lmstudio` | OpenAI compat (local) | `LM_STUDIO_BASE_URL` |
| `llamacpp` | Anthropic native (local) | `LLAMACPP_BASE_URL` |
| `ollama` | OpenAI compat (local) | `OLLAMA_BASE_URL` |

---

## Admin UI

Local-only (loopback guard), served at `/admin`.

| File | Purpose |
|---|---|
| `api/admin_static/index.html` | Shell: sidebar, views, toast region, action bar |
| `api/admin_static/admin.css` | Design system, animations, toast, skeleton, responsive |
| `api/admin_static/admin.js` | Toast, auto-refresh, keyboard shortcuts, copy, dirty guard |

**Features shipped:**
- Toast notification system (ok / warn / error / info, auto-dismiss 4.2 s)
- Provider status auto-refresh every 30 s with pulsing indicator
- `Ctrl+S` → validate · `Ctrl+Enter` → apply
- Copy-to-clipboard on non-secret text fields
- Skeleton loading cards on boot
- `beforeunload` unsaved-changes browser guard
- 6 CSS keyframe animations; accent glow on focused inputs
- Full mobile-responsive layout

---

## CI / Quality Gates

All enforced in `.github/workflows/tests.yml` on push/PR to `main`/`master`:

| Job | Command | Status |
|---|---|---|
| Ban type ignore suppressions | `grep -rE '# type: ignore\|# ty: ignore'` | ✅ |
| ruff-format | `uv run ruff format --check` | ✅ |
| ruff-check | `uv run ruff check` | ✅ |
| ty | `uv run ty check` | ✅ |
| pytest | `uv run pytest -v --tb=short` | ✅ 1429/1429 |

---

## Roadmap

### Completed ✅
- [x] FastAPI proxy with Anthropic-compatible routes
- [x] 17 provider backends (OpenAI compat + Anthropic native + local)
- [x] Per-model-tier routing (Opus / Sonnet / Haiku / fallback)
- [x] Streaming, tool use, thinking block support
- [x] Request optimization handlers (probe mocking, title skip, etc.)
- [x] Admin UI — config editor, provider status, validate/apply
- [x] Discord + Telegram bot wrapper with session trees
- [x] Voice note transcription (local Whisper + NVIDIA NIM Riva)
- [x] `/v1/models` Gateway model discovery
- [x] Rate limiting + concurrency control per provider
- [x] **Production Admin UI overhaul** — Toast, auto-refresh, kbd shortcuts, copy, skeleton
- [x] **Real-time settings search/filter** — topbar input, key+label match, auto-hides empty sections

### Planned 🔲
- [ ] Dark/light theme toggle in Admin UI
- [ ] Per-request latency and token-usage metrics panel
- [ ] Provider health history (sparkline chart)
- [ ] One-click provider API key validation (not just model fetch)

---

## Changelog

### v2.0.0 — 2026-05-30
- **Real-time settings search/filter**: topbar `<input type="search">`, filters all `.field` elements live by env-var key + human label; empty section cards auto-hide; clears on view-switch

### v2.0.0 — 2026-05-29
- **Production Admin UI overhaul**: Toast system, 30 s auto-refresh with pulsing dot, `Ctrl+S` / `Ctrl+Enter` shortcuts, copy-to-clipboard, skeleton loading, unsaved-changes guard, 6 CSS keyframe animations, provider card lift-on-hover, accent glow on focused inputs, full mobile reflow
- Fixed `.gitignore` `server.*` wildcard over-matching `server.py` — added `!server.py` negation
- 1429 / 1429 tests passing

### v2.0.0 (upstream baseline)
- 17 provider backends
- Messaging platforms (Discord, Telegram), voice transcription
- Admin UI for managed `.env` config
- Gateway model discovery via `/v1/models`
- Python 3.14, uv, ruff, ty CI stack
