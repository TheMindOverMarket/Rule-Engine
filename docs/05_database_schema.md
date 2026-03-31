# Database Schema

The engine relies on Supabase to persist compiled structured logic.

## Core Tables
1. **`playbooks`**: Represents an entire trading strategy. Stores the original natural language, the compiled Context Skeleton JSON, and the overarching status (`COMPLETED`).
2. **`playbook_rules`**: A single `RuleBlock` entity (Category-specific).
3. **`rule_conditions`**: The specific boolean logic gates (ALL, ANY, NONE).
4. **`rule_extensions`**: Extends conditions into specific primitives (e.g., mapping to the `rate_limit` Primitive with specific max limits).

## Engine Population
The `populate_tables.py` script bridges the gap between memory and Supabase. Once the LLM generates a Playbook in memory, the populate script walks the Playbook tree and writes the nested rules, conditions, and extensions directly into the database schema for later execution.
