# TMOM Rule Engine ⚙️

The TMOM Rule Engine is a real-time deterministic trading strategy supervisor. It parses natural language trading rules, compiles them into a structured database schema, and executes them against live market and account data. 

When a trader breaks their own rules (e.g. trading during a cooldown, risking too much, or holding past a stop loss), the Engine instantly flags the deviation.

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- A [Supabase](https://supabase.com/) project (for storing playbooks/rules)
- An [Alpaca](https://alpaca.markets/) account (for real-time equity and buying power data)
- OpenAI API Key (for the LLM Parser)

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd tmom/Rule-Engine
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```
*(If you do not have a requirements file yet, ensure you have dependencies like `aiohttp`, `websockets`, `pandas`, `alpaca-trade-api`, `supabase`, `pydantic`, `openai`, and `python-dotenv` installed)*

### 3. Environment Variables
Create a `.env` file in the root of the `Rule-Engine` directory:
```env
# TMOM API / Engine Config
TMOM_BACKEND_BASE_URL="https://tmom-app-backend.onrender.com"
PORT=8080

# Broker (Alpaca) Config
API_KEY="your_alpaca_key"
SECRET_KEY="your_alpaca_secret"

# LLM Config
OPENAI_KEY="your_openai_key"

# Database (Supabase) Config
SUPABASE_URL="your_supabase_url"
SUPABASE_SERVICE_ROLE_KEY="your_supabase_service_key"
```

### 4. Running the Engine
To start the live execution loop and WebSocket servers:

```bash
python main.py
```
This process acts as the gateway. Depending on your configuration, it bootstraps the LLM parser, interacts with Supabase natively to populate playbook tables, and automatically spins up the `execution_engine.py` background tasks that listen to the `wss://` market streams.

---

## 📖 Engine Architecture & Documentation
To completely understand the inner workings, please read through the **[docs/ folder](docs/)** in chronological order:

1. [Architectural Overview](docs/00_architecture_overview.md) - High-level system design.
2. [Rule Parsing](docs/01_rule_parsing.md) - How natural language becomes a Context Skeleton.
3. [Execution Engine](docs/02_execution_engine.md) - The WebSocket loops & data hydration.
4. [Deviation Workflow](docs/03_deviation_workflow.md) - Action-Gated limits vs. State-Mandated exits.
5. [Primitives Guide](docs/04_primitives_guide.md) - How to extend the engine with new logic components.
6. [Database Schema](docs/05_database_schema.md) - Table mapping for Playbooks.

---

## 🧠 Core Concepts at a Glance

* **Primitive**: The smallest atomic evaluator block (e.g., `comparison`, `temporal_gate`, `rate_limit`). Requires specific data context to run.
* **Extension**: A specific configuration of a primitive (e.g., `rate_limit` with `max=5`).
* **RuleBlock**: A collection of extensions connected by explicit logic gates (`ALL`, `ANY`, `NONE`). Organized by Categories (`ENTRY`, `RISK`, `DISCIPLINE`, `EXIT`).
* **ContextBuilder**: Hydrates the fast market tick with slow account data (equity) before evaluation.
* **Logic Adherence**: The engine component that analyzes evaluated rules and strictly flags deviations if a trader disobeys limits or mandatory exits.

---

## 🛠 Active To-Do List & Upgrades
- Finalize the automated data-cleaning pipeline for market ingestion buffers (handle API downtime seamlessly).
- Implement broader limits tracking for AWS/Cloud provider services natively.
- Implement robust random-jitter reconnection logic for WebSockets (to avoid thundering herd failures upon backend disconnects).
- Ensure fewer redundant WebSocket connections are sent to upstream providers (e.g. Binance/Alpaca) by multiplexing.
- Build a mechanism to dynamically deactivate individual rules (like entries) chronologically as the trading session matures.
- Implement token expiration and dynamic revocation for WebSocket authentication.
