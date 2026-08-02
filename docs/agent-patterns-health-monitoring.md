# Agent Patterns: Building Health-Monitoring Agents with DataHub MCP

This guide demonstrates how to build a production-grade health-monitoring agent over DataHub's context graph using the MCP Server. The pattern is battle-tested in the [datahub-rail-agent](https://github.com/kevinmasterson/datahub-rail-agent) reference implementation.

## Overview

A health-monitoring agent runs **structured probes** across your data estate to detect and classify failures, then walks your lineage graph to triage root causes and generate owner-addressed incident reports. The key challenge: distinguishing signal from noise in alert-heavy environments.

## Core Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Agent Loop                                              │
├─────────────────────────────────────────────────────────┤
│ 1. Probe Phase         2. Triage Phase   3. Report Phase│
│    ↓                       ↓                 ↓           │
│ · Freshness probe   → · Walk lineage   → · Generate MD  │
│ · Lineage probe     → · Pick root-cause  · Assign owners│
│ · Schema probe      → · Gather evidence   · Alerts      │
│                                                          │
│ MCP Tools Used:                                         │
│ · search() – find datasets in registry                  │
│ · get_lineage() – explore dependencies                  │
│ · call_tool() – graph reads                             │
└─────────────────────────────────────────────────────────┘
```

## Pattern 1: Capture-Based Freshness Probes

**Principle:** Never query job heartbeats; measure data's own recency via `lastModified` metadata.

```python
class FreshnessProbe:
    """Capture-based freshness check."""
    
    def __init__(self, sla_hours: int = 24):
        self.sla_hours = sla_hours
    
    async def run(self, dataset_urn: str, client: MCPClient) -> ProbeResult:
        """Check if dataset is fresh relative to SLA."""
        try:
            freshness = await client.get_freshness(dataset_urn)
            
            age_hours = (now() - freshness.last_modified).total_seconds() / 3600
            
            if age_hours > self.sla_hours:
                return ProbeResult(
                    status="fail",
                    message=f"Stale ({age_hours:.1f}h old, SLA: {self.sla_hours}h)"
                )
            return ProbeResult(status="pass", message="Fresh")
            
        except Exception as e:
            # Never raise; return actionable failure
            return ProbeResult(
                status="fail",
                message=f"Freshness check unavailable: {e}"
            )
```

**Why this works:**
- `lastModified` is authoritative data, not a signal about a job scheduler
- No external job-state polling, no stale workflow metadata
- Graceful failure: unreachable states return `fail`, not silent passes

## Pattern 2: Never-Raise Contract

All probes follow a strict contract: **catch all exceptions, return a ProbeResult with a message**. This ensures a single failed lineage lookup doesn't crash your entire monitoring run.

```python
class LineageProbe:
    """Detect broken/missing upstream dependencies."""
    
    async def run(self, dataset_urn: str, client: MCPClient) -> ProbeResult:
        """Walk upstream 1 hop; detect missing edges."""
        try:
            lineage = await client.walk_upstream(dataset_urn, hops=1)
            
            if not lineage.nodes:
                # Watch blind: no upstream lineage data
                return ProbeResult(
                    status="fail",
                    message="Watch blind: no upstream lineage found"
                )
            
            # Check for missing edges (lineage pointing to deleted datasets)
            for edge in lineage.edges:
                if not edge.target_exists:
                    return ProbeResult(
                        status="fail",
                        message=f"Broken edge: upstream {edge.target_urn} deleted"
                    )
            
            return ProbeResult(status="pass", message="Lineage intact")
            
        except Exception as e:
            # Lineage data may be unavailable; report it, don't crash
            return ProbeResult(
                status="fail",
                message=f"Lineage check failed: {e}"
            )
```

## Pattern 3: Delta-Aware State History

**The Alarm-Fatigue Killer:** Render alerts on *change*, not raw thresholds. Persist a JSONL history of probe results per dataset, and classify failures as:

- **NEW**: First occurrence (triggers urgency)
- **CHRONIC**: Still failing after N hours (deprioritized, tracked)
- **RECOVERED**: Flip from fail→pass (close the incident)

```python
class StateHistory:
    """JSONL-persisted state with bounded rotation."""
    
    def __init__(self, path: str, max_entries: int = 100):
        self.path = path
        self.max_entries = max_entries
    
    async def append(self, dataset_urn: str, probe_result: ProbeResult):
        """Append probe result; rotate if needed."""
        entry = {
            "timestamp": now().isoformat(),
            "dataset_urn": dataset_urn,
            "status": probe_result.status,
            "message": probe_result.message,
        }
        
        with open(self.path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        
        # Bounded rotation: keep recent entries only
        if self._entry_count() > self.max_entries:
            self._rotate()
    
    def render_digest(self, dataset_urn: str) -> str:
        """Classify failure as NEW, CHRONIC, or RECOVERED."""
        history = self._load_history(dataset_urn)
        
        if not history:
            return "First run: no history"
        
        recent_statuses = [h["status"] for h in history[-7:]]
        
        if recent_statuses[0] == "fail" and recent_statuses[-1] == "pass":
            return "RECOVERED — check root cause resolution"
        elif all(s == "fail" for s in recent_statuses):
            return "CHRONIC — still failing after 7 runs"
        elif recent_statuses[-1] == "fail":
            return "NEW — investigate immediately"
        else:
            return ""  # No alert needed
```

**Result:** Alerts fire on meaningful change, not every SLA miss. Chronic failures are tracked but deprioritized.

## Pattern 4: Lineage-Walk Root-Cause Triage

When a probe fails, walk upstream to find the **deepest failing node** (furthest from the leaf failure). This is the root-cause candidate.

```python
async def triage_failure(
    failing_dataset_urn: str, 
    client: MCPClient, 
    probes: dict[str, Probe]
) -> RootCauseReport:
    """Walk upstream; find deepest failing node."""
    
    # BFS walk to collect ancestors
    visited = set()
    queue = [(failing_dataset_urn, 0)]  # (urn, depth)
    ancestors = {}
    
    while queue:
        urn, depth = queue.pop(0)
        if urn in visited:
            continue
        visited.add(urn)
        ancestors[urn] = depth
        
        # Walk up 1 more hop
        if depth < 5:
            lineage = await client.walk_upstream(urn, hops=1)
            for parent_urn in lineage.parent_urns:
                if parent_urn not in visited:
                    queue.append((parent_urn, depth + 1))
    
    # Find deepest failing node (run probes on ancestors)
    failing_nodes = []
    for urn, depth in ancestors.items():
        for probe in probes.values():
            result = await probe.run(urn, client)
            if result.status == "fail":
                failing_nodes.append((urn, depth, result))
                break
    
    if not failing_nodes:
        return RootCauseReport(
            failing_dataset=failing_dataset_urn,
            root_cause=None,
            reason="No upstream failures detected"
        )
    
    # Pick deepest (furthest from leaf)
    root_urn, root_depth, root_result = max(
        failing_nodes, 
        key=lambda x: x[1]
    )
    
    # Gather evidence from graph
    metadata = await client.get_metadata(root_urn)
    lineage_path = await client.walk_upstream(root_urn, hops=3)
    
    return RootCauseReport(
        failing_dataset=failing_dataset_urn,
        root_cause=root_urn,
        owner=metadata.owner,
        evidence={
            "probe_message": root_result.message,
            "lineage_path": lineage_path,
            "depth_in_dag": root_depth,
        }
    )
```

**Key insight:** The deepest failing node is the likely root cause; shallower nodes may have failed *because* of it.

## Pattern 5: Provenance Guarantee

**All facts in reports come from graph reads.** The LLM only phrases the narrative.

```python
# ✓ GOOD: Fact sourced from graph
owner = await client.get_ownership(dataset_urn)
message = f"Dataset owned by {owner.name}. Last modified {freshness.last_modified}."

# ✗ BAD: LLM hallucinated detail
message = llm_generate(f"Write a report for {dataset_urn}")  # May confabulate
```

Structure your report generation to separate:
1. **Graph reads** (facts)
2. **Report template** (narrative structure)
3. **LLM role** (phrasing only, never data sourcing)

```python
async def generate_report(
    root_cause_urn: str,
    client: MCPClient
) -> str:
    # Step 1: Gather all facts from graph
    metadata = await client.get_metadata(root_cause_urn)
    ownership = await client.get_ownership(root_cause_urn)
    lineage = await client.walk_upstream(root_cause_urn, hops=3)
    
    # Step 2: Structure report with facts only
    report = f"""
## Incident Report

**Root-Cause Dataset:** {metadata.name}
**Owner:** {ownership.name}
**Last Modified:** {metadata.last_modified}

**Upstream Path:**
{" → ".join([node.name for node in lineage.nodes])}

**Next Steps:**
Contact {ownership.name} to investigate.

---
*All facts sourced from DataHub context graph reads.*
"""
    
    return report
```

## Integration with MCP Client

```python
from mcp import ClientSession, StdioServerParameters, stdio_client

async def run_monitoring_agent():
    """End-to-end agent loop."""
    
    params = StdioServerParameters(
        command="uvx",
        args=["mcp-server-datahub@latest"],
    )
    transport, session = await stdio_client(params)
    
    # Load your probes
    probes = {
        "freshness": FreshnessProbe(sla_hours=24),
        "lineage": LineageProbe(),
        "schema": SchemaProbe(),
    }
    
    # Search for datasets to monitor
    results = await session.call_tool("search", {
        "query": "/q tag:monitored",
        "limit": 50,
    })
    
    # Run probes → triage → report
    for dataset in results["entities"]:
        dataset_urn = dataset["urn"]
        
        # Phase 1: Probes
        probe_results = {}
        for name, probe in probes.items():
            result = await probe.run(dataset_urn, session)
            probe_results[name] = result
        
        # Phase 2: If any probe failed, triage
        if any(r.status == "fail" for r in probe_results.values()):
            root_cause_report = await triage_failure(
                dataset_urn, session, probes
            )
            
            # Phase 3: Generate and send report
            report = await generate_report(
                root_cause_report.root_cause, 
                session
            )
            print(report)
            # → send via Slack, email, incident system, etc.
    
    await session.close()
```

## Best Practices

1. **Bounded Timeouts:** Set `timeout_seconds` on all MCP calls. DataHub's lineage walks can be expensive.
   ```python
   lineage = await asyncio.wait_for(
       client.walk_upstream(urn, hops=3),
       timeout=10.0
   )
   ```

2. **Caching:** Cache owner/metadata lookups; they're expensive and change slowly.
   ```python
   owner_cache = {}
   if urn not in owner_cache:
       owner_cache[urn] = await client.get_ownership(urn)
   ```

3. **Error Budgets:** Plan for DataHub unavailability. Graceful degradation beats crash loops.
   ```python
   try:
       lineage = await client.walk_upstream(urn, hops=2)
   except Exception:
       # Fall back to shallow checks or skip
       lineage = None
   ```

4. **Deduplicate Alerts:** Don't fire the same incident twice. Use dataset_urn + fault_type as the incident key.
   ```python
   incident_key = f"{dataset_urn}:freshness-sla"
   if incident_key not in deduplication_set:
       send_alert(incident_key, report)
   ```

## Reference Implementation

See [datahub-rail-agent](https://github.com/kevinmasterson/datahub-rail-agent) for a complete working example:
- Multi-probe engine with schema drift detection
- Delta-aware alerting with state history
- Incident report generation with owner assignment
- Configuration-driven probe registry
- Test-driven implementation with 80+ passing tests

## Questions?

Open an issue on the [DataHub community forums](https://discuss.datahub.project.io/) or the MCP Server [GitHub repository](https://github.com/acryldata/mcp-server-datahub).
