# Rule Parsing & Compilation

The engine requires structured logic to execute safely. `RuleParser` uses Large Language Models to convert natural language into two critical components:

## 1. Context Skeleton
The skeleton tells the `ContextBuilder` exactly what data it must fetch to evaluate the rules. It includes necessary indicators, account fields (`equity`, `daytrade_count`), and required history metrics. 

By defining this upfront, the execution engine avoids fetching massive, unnecessary datasets on every tick.

## 2. The Playbook
A `Playbook` is a collection of `RuleBlock` objects organized by `RuleCategory` (ENTRY, PROCESS, RISK, DISCIPLINE, EXIT).

### Example Parsing Workflow
1. **User Input**: "Max 5 trades per day"
2. **LLM Output**: Category: `RISK`, Primitive: `rate_limit`, Params: `{"max": 5}`
3. **Compilation**: The parsed JSON is mapped to Supabase tables by `populate_playbook_tables()`. The Execution Engine later reconstructs it into executable Python objects.
