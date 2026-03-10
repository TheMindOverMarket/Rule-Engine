# primitives.py
from typing import Dict, Any
from datetime import datetime

def parse_time_to_seconds(time_val: Any) -> float:
    """
    Converts a time value (int seconds or ISO string) to seconds since midnight.
    Format: YYYY-MM-DDTHH:MM:SS.mmmZ
    """
    if isinstance(time_val, (int, float)):
        return float(time_val)
    
    if isinstance(time_val, str):
        try:
            # Handle Z suffix
            clean_val = time_val.replace('Z', '+00:00')
            dt = datetime.fromisoformat(clean_val)
            seconds = dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
            return seconds
        except ValueError:
            pass
    return 0.0

# ---------------------------
# Core Primitive Evaluators
# ---------------------------

# Change as needed as this is recommendation from GPT
# Might need Satya knowledge on this LOL

def comparison_evaluator(params: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Evaluates comparison between a context value (left) and a constant (right)."""
    left = context.get(params['left'], 0)
    right = params['right']
    op = params['op']
    
    # If right is a string, it might be a reference or arithmetic expression (e.g. "VWAP + 1.5 * ATR_14")
    if isinstance(right, str):
        if right in context:
            right = context[right]
        else:
            # Check if it's an arithmetic expression we need to evaluate safely
            import re
            
            # Simple check if there's any math operator in the string
            if any(char in right for char in ['+', '-', '*', '/']):
                # Find all potential variable names (words/identifiers) in the string
                # Regex matches alphanumeric strings including underscores
                vars_in_expr = set(re.findall(r'[a-zA-Z_]\w*', right))
                
                expr = right
                can_evaluate = True
                
                for var in vars_in_expr:
                    if var in context:
                        # Substitute the variable securely with its numeric value
                        val = context[var]
                        expr = expr.replace(var, str(val))
                    else:
                        # Missing a required variable to compute the math
                        can_evaluate = False
                        break
                        
                if can_evaluate:
                    try:
                        # Safely evaluate simple math expressions
                        # Only allow math structures (no builtins/functions) execution
                        right = eval(expr, {"__builtins__": None}, {})
                    except Exception as e:
                        pass # Keep right as original string if eval fails

    # Safely try to cast string numbers (like "100000" from Alpaca or LLM params) to floats
    # This prevents [EVALUATOR WARNING] Type mismatch: <class 'float'> vs <class 'str'>
    def safe_to_float(val):
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            try:
                return float(val)
            except ValueError:
                return 0.0 # If it's a completely unresolved variable, default to 0.0
        return 0.0

    left = safe_to_float(left)
    right = safe_to_float(right)

    if op == '>':
        return left > right
    elif op == '<':
        return left < right
    elif op == '==':
        return left == right
    elif op == '>=':
        return left >= right
    elif op == '<=':
        return left <= right
    else:
        raise ValueError(f"Unknown operator {op}")


def set_membership_evaluator(params: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Checks if a context value is within allowed set or excludes forbidden set."""
    value = context.get(params['field'])
    allowed = params.get('allowed', [])
    forbidden = params.get('forbidden', [])

    if allowed and value not in allowed:
        return False
    if forbidden and value in forbidden:
        return False
    return True


def rate_limit_evaluator(params: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Checks if event count within a time window exceeds a limit."""
    metric = params['metric']
    max_count = params['max']
    window_minutes = params['window_minutes']

    history = context.get('history', {}).get(metric, [])  # list of timestamps
    current_time_val = context.get('current_time')
    
    current_seconds = parse_time_to_seconds(current_time_val)

    # Assuming history items are compatible (also ISO or seconds)
    # We count how many t are within window
    count = 0
    for t in history:
        t_seconds = parse_time_to_seconds(t)
        if current_seconds - t_seconds <= window_minutes * 60:
            count += 1
            
    return count <= max_count


def accumulation_evaluator(params: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Evaluates if an accumulated metric (e.g. daily loss) meets a threshold."""
    field = params['field']
    threshold = params['threshold']
    op = params.get('op', '>=')

    total = context.get(field, 0)

    if isinstance(threshold, str):
        try:
            threshold = float(threshold)
        except ValueError:
            pass

    if op == '>=':
        return total >= threshold
    elif op == '<=':
        return total <= threshold
    elif op == '>':
        return total > threshold
    elif op == '<':
        return total < threshold
    elif op == '==':
        return total == threshold
    else:
        raise ValueError(f"Unknown operator {op}")


def sequence_evaluator(params: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """
    Detects if a specific sequence of events occurred directly in history.
    Optionally constrained by a time window.
    """
    pattern = params['pattern']
    window_minutes = params.get('window_minutes', 0)

    events = context.get('event_history', [])
    if window_minutes:
        current_time_val = context.get('current_time')
        current_seconds = parse_time_to_seconds(current_time_val)
        
        # Filter events by window
        filtered_events = []
        for t, e in events:
            t_seconds = parse_time_to_seconds(t)
            if current_seconds - t_seconds <= window_minutes * 60:
                filtered_events.append((t, e))
        events = filtered_events

    events_only = [e for _, e in events]
    pattern_index = 0

    for e in events_only:
        if e == pattern[pattern_index]:
            pattern_index += 1
            if pattern_index == len(pattern):
                return True

    return False


def temporal_gate_evaluator(params: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Evaluates if current time is within allowed window or past a cooldown."""
    current_time_val = context.get('current_time')
    current_seconds = parse_time_to_seconds(current_time_val)

    start = params.get('start_time')
    end = params.get('end_time')

    if start and end:
        return start <= current_seconds <= end

    cooldown_end = params.get('cooldown_end')
    if cooldown_end:
        return current_seconds >= cooldown_end

    return True

def account_comparison_evaluator(params: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """
    Evaluates a comparison against a broker account field.

    Example params:
    {
        "field": "buying_power",
        "op": ">=",
        "value": 50000
    }

    Requires that `context` contains an "account" dict with the relevant field.
    """
    account = context.get("account", {})
    
    field = params["field"]
    op = params["op"]
    value = params["value"]

    if field not in account:
        raise ValueError(f"Account field '{field}' not available in context")
    if isinstance(value, str) and not any(c.isalpha() for c in value):
        value = float(value)
    elif isinstance(value, (int, float)):
        pass
    else:
        pass

    # If value is explicitly None (e.g. LLM failed to resolve), we cannot compare.
    if value is None:
        print(f"WARNING: account_comparison_evaluator received None for 'value' on field '{field}'. Returning False.")
        return False

    # Simplified logic to match intent safely:
    if isinstance(value, str):
         if not any(c.isalpha() for c in value):
             value = float(value)
         else:
             # Fallback to original behavior for string with alphas, though suspicious
             value = account.get('value', 0) # protecting against KeyError if 'value' missing
    # if it's int/float, leave it as is.


    account_value = float(account[field])

    if op == ">":
        return account_value > value
    elif op == ">=":
        return account_value >= value
    elif op == "<":
        return account_value < value
    elif op == "<=":
        return account_value <= value
    elif op == "==":
        return account_value == value
    else:
        raise ValueError(f"Unknown operator '{op}' in account_comparison")
