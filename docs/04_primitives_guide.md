# Primitives Guide

`engine.py` is built entirely on Primitives. 

A Primitive is a single base unit of logic that wraps an `evaluator()` function and strictly defines its context requirements. The LLM does not write logic; it maps natural language directly to your predefined primitives.

## Structure of a Primitive
```python
Primitive(
    name="comparison",
    evaluator=comparison_evaluator,
    required_context=["price"], 
    required_account_fields=["equity"] # Optional
)
```

## Creating a New Primitive
If a user wants a rule type that isn't supported, you must manually add a Primitive:

1. **Write the Evaluator:**
    ```python
    def volume_spike_evaluator(params: dict, context: dict) -> bool:
        return context.get("volume", 0) > params.get("threshold", 1000)
    ```
2. **Register It:**
    ```python
    PrimitiveRegistry.register(
        Primitive("volume_spike", volume_spike_evaluator, required_context=["volume"])
    )
    ```
3. **Update LLM Prompts:** Ensure `prompts.py` informs the LLM that the `"volume_spike"` primitive exists.
