# Deviation Workflow

The primary goal of `logic_adherence.py` is to track when a trader deviates from their defined Playbook rules.

The system separates rules into two fundamental tracking types based on their underlying primitive algorithms:

## 1. Action-Gated Rules
These rules dictate precisely **when you are allowed to pull the trigger**. 
Mathematically, they require the trader to take a manual action to incur a deviation. Sitting still and doing nothing is always compliant.

- **Examples:** 
  - "10-minute cooldown after a loss" (Discipline/Cooldown)
  - "Max 5 trades per day" (Risk/Rate Limit)
  - "EMA slope > 0" (Entry/Permissive Guardrail)
- **The Equation:** `Deviation = user_action_bool AND NOT rule_is_true`

## 2. State-Mandated Rules
These are rules that dictate a state you **must not remain in**. They require the trader to immediately take action to get out of a dangerous position.
Mathematically, failing to act triggers continuous deviation penalties.

- **Examples:** 
  - "Stop loss at 1 ATR" (Risk/Stop)
  - "Close positions at 3:55 PM" (Exit/Time)
- **The Equation:** `Deviation = NOT rule_is_true` 
*(Completely ignores `user_action_bool`. As long as the forbidden state persists, the trader accumulates deviation penalties on every market tick).*

## The Output Payload
On every tick, the frontend WebSocket receives a payload specifying:
- `deviation`: Overall boolean metric for the tick.
- `deviation_true`: List of specific rule IDs that were violated.
- `accumulated_deviation`: Running counter of total deviated ticks.
