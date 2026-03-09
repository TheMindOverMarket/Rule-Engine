import aiohttp
import urllib.parse
from engine import Playbook, RuleBlock
from typing import Dict, Any

API_BASE_URL = "https://tmom-app-backend.onrender.com"

async def create_rule(session: aiohttp.ClientSession, playbook_id: str, name: str) -> str:
    """Creates a new Rule in the database."""
    url = f"{API_BASE_URL}/rules/"
    payload = {
        "name": name,
        "playbook_id": playbook_id,
        "is_active": True
    }
    async with session.post(url, json=payload) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return data["id"]

async def create_condition(
    session: aiohttp.ClientSession, 
    rule_id: str, 
    metric: str, 
    comparator: str, 
    value: str
) -> str:
    """Creates a new Condition in the database."""
    url = f"{API_BASE_URL}/conditions/"
    payload = {
        "rule_id": rule_id,
        "metric": metric,
        "comparator": comparator,
        "value": str(value),
        "is_active": True
    }
    async with session.post(url, json=payload) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return data["id"]

async def create_condition_edge(
    session: aiohttp.ClientSession,
    rule_id: str,
    parent_condition_id: str,
    child_condition_id: str,
    logical_operator: str
):
    """Creates a ConditionEdge linking two conditions in the database."""
    url = f"{API_BASE_URL}/condition-edges/"
    payload = {
        "rule_id": rule_id,
        "parent_condition_id": parent_condition_id,
        "child_condition_id": child_condition_id,
        "logical_operator": logical_operator.upper()
    }
    async with session.post(url, json=payload) as resp:
        resp.raise_for_status()
        return await resp.json()

async def populate_playbook_tables(user_id: str, playbook_id: str, playbook: Playbook):
    """
    Takes a parsed Playbook and populates the backend database tables
    (rules, conditions, condition-edges).
    """
    print(f"[TABLE POPULATION] Starting database population for playbook {playbook_id}")
    
    async with aiohttp.ClientSession(headers={"accept": "application/json", "Content-Type": "application/json"}) as session:
        for rule in playbook.rules:
            try:
                # 1. Create Rule
                rule_id = await create_rule(session, playbook_id, rule.name)
                print(f"  [+] Created Rule ID: {rule_id} for '{rule.name}'")
                
                # 2. Extract and Create Conditions
                ext_id_to_cond_id = {}
                for ext_id, ext in rule.extensions.items():
                    # Attempt to safely extract metric, comparator, and value from the primitive param structure
                    # Primitive structures might vary slightly, so we use fallbacks.
                    # e.g., comparison primitive has field/metric, op, value.
                    
                    params = ext.params
                    # 'field' or 'metric' contains what we are checking against
                    metric = params.get("field") or params.get("metric", "Unknown_Metric")
                    if isinstance(metric, list) and len(metric) > 0:
                        metric = metric[0] # Take first field if it's a list

                    comparator = params.get("op", "==")
                    value = str(params.get("value", ""))
                    
                    try:
                        cond_id = await create_condition(session, rule_id, metric, comparator, value)
                        ext_id_to_cond_id[ext_id] = cond_id
                        print(f"    [+] Created Condition ID: {cond_id} ({metric} {comparator} {value})")
                    except Exception as cond_err:
                        print(f"    [-] Failed to create Condition: {cond_err}")
                        continue
                
                # 3. Recursively create edges based on rule.conditions tree
                async def traverse_conditions(node: Any, parent_cond_id: str = None, connection_logic: str = "AND") -> list:
                    """
                    Returns a list of condition IDs nested under this logical node.
                    This simplifies edge creation. For top-level node, it builds edges between the children.
                    """
                    if isinstance(node, str):
                        # Leaf node (Extension ID)
                        cond_id = ext_id_to_cond_id.get(node)
                        if parent_cond_id and cond_id and parent_cond_id != cond_id:
                            # Typically the DB schema models linear dependencies or an explicit tree. 
                            # If we require edges, we link parent -> child
                            await create_condition_edge(session, rule_id, parent_cond_id, cond_id, connection_logic)
                            print(f"      [->] Edge (Parent: {parent_cond_id} -> Child: {cond_id} | OP: {connection_logic})")
                        return [cond_id] if cond_id else []
                    
                    if not isinstance(node, dict):
                        return []
                    
                    all_nodes = node.get("all", [])
                    any_nodes = node.get("any", [])
                    
                    child_ids = []
                    
                    # 'AND' nodes
                    if all_nodes:
                        children = []
                        for n in all_nodes:
                            ids = await traverse_conditions(n, parent_cond_id, "AND")
                            children.extend(ids)
                        
                        # In many rule engines, if you have a list of AND conditions, 
                        # they can be chained linearly OR all joined to a logic gate.
                        # For this schema lacking explicit gate-entities, we'll link them linearly
                        # if there's no parent, just to connect them in the graph.
                        if len(children) > 1 and parent_cond_id is None:
                            for i in range(len(children) - 1):
                                await create_condition_edge(session, rule_id, children[i], children[i+1], "AND")
                                print(f"      [->] Edge ({children[i]} AND {children[i+1]})")
                        child_ids.extend(children)
                        
                    # 'OR' nodes
                    if any_nodes:
                        children = []
                        for n in any_nodes:
                            ids = await traverse_conditions(n, parent_cond_id, "OR")
                            children.extend(ids)
                            
                        # Link OR conditions linearly if no parent
                        if len(children) > 1 and parent_cond_id is None:
                             for i in range(len(children) - 1):
                                await create_condition_edge(session, rule_id, children[i], children[i+1], "OR")
                                print(f"      [->] Edge ({children[i]} OR {children[i+1]})")
                        child_ids.extend(children)
                        
                    return child_ids
                
                # Start traversal to build edges
                print(f"    [*] Building Condition Edges...")
                await traverse_conditions(rule.conditions)
                print(f"  [=] Finished Rule processing.")
                    
            except Exception as e:
                print(f"  [-] Failed to process rule '{rule.name}': {e}")
                
    print(f"[TABLE POPULATION] Finished.")
