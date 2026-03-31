# Execution Engine

`execution_engine.py` orchestrates the live trading session. 

## The Dual-WebSocket Loop
The engine opens two connections:
1. **Market State Stream**: Receives fast, real-time price and indicator updates.
2. **User Activity Stream**: Receives manual overrides and broker events (e.g., when the trader manually clicks "BUY").

## Core Loop Steps

### 1. Context Hydration
On every market tick, `ContextBuilder.hydrate()` runs. It smartly merges:
- **Fast Data**: Incoming market payload (Price, indicators).
- **Slow Data**: Account data fetched from the `AccountProvider` (e.g., Alpaca buying power).
- **Session Data**: Trade history for the current run.

### 2. Evaluation
`playbook.evaluate(full_context)` runs all compiled logic across all categories using the hydrated data dictionary.

### 3. Output & Persistence
The true/false results are passed to `build_logic_adherence_payload()`, which maps evaluations to trader deviations. The final payload is broadcast to listening frontend clients and stored in the backend database.
