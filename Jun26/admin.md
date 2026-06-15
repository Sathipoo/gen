Absolutely — revised with **admins as the audience**, focused on **what they need to implement and monitor**.

---

```text
# Slide 1: Problem Statement & Objective

## Intermittent SQL Server Timeout Failures in IICS

- IICS jobs are intermittently failing while connecting to SQL Server.
- Failure log shows timeout-related errors.
- The same job succeeds when manually restarted without any code/configuration change.
- This indicates a transient SQL Server/network connectivity issue.
- Current workaround is manual rerun.
- Objective is to enable automatic retry so eligible timeout failures are retried before the job is marked failed.

Reference:
- Informatica Case: 04996054
- Product: IICS Cloud Data Integration
- Database: Microsoft SQL Server
```

```text
# Slide 2: Solution 1 – Connection Level Configuration

## Preferred Solution: Apply Retry Settings at SQL Server Connection Level

Admins to configure retry/timeout properties on the affected SQL Server connection.

Purpose:
- Increase wait time for SQL Server connection establishment.
- Enable retry for transient connection timeout failures.
- Avoid job failure when SQL Server/network issue clears within retry window.

Recommended configuration approach:

| Parameter | Purpose | Proposed Value |
|---|---|---|
| Login / connection timeout | Wait longer before connection fails | 60–120 sec |
| Retry enablement | Enable retry for transient failures | Enabled |
| Retry count | Number of retry attempts | 3 |
| Retry interval | Wait between retries | 30–60 sec |

Expected outcome:
- Job does not fail immediately on first timeout.
- IICS retries the SQL Server connection.
- If retry succeeds, job continues without manual restart.
- If all retries fail, job fails normally.
```

```text
# Slide 3: Solution 2 – Platform / Org Level Configuration

## Alternative Solution: Apply Retry Settings at Platform / Org Level

If connection-level configuration is not sufficient or not supported, admins can apply retry/timeout configuration at platform/org level.

This may require Secure Agent / runtime service restart and coordination with other teams.

Use this option when:
- Multiple SQL Server jobs are affected.
- Same timeout pattern is observed across multiple connections/projects.
- Connection-level setting is not available or does not solve the issue.

Considerations:

| Area | Impact |
|---|---|
| Scope | May affect multiple projects/connections |
| Restart | Secure Agent/runtime restart may be required |
| Downtime | Needs planned maintenance window |
| Risk | Wider blast radius compared to connection-level setting |

Recommendation:
- Try Solution 1 first.
- Move to Solution 2 only if connection-level retry does not resolve the recurring failures.
```

```text
# Slide 4: Implementation Plan for Admins

## Proposed Rollout Approach

Step 1: Identify affected SQL Server connection
- Confirm connection used by failing job.
- Confirm whether failure occurs at source, target, lookup, or stored procedure connection.

Step 2: Apply Solution 1
- Add retry/timeout properties at SQL Server connection level.
- Save and validate the connection.
- Run affected task in lower environment.

Step 3: Controlled production implementation
- Apply same connection-level configuration in production.
- No mapping or task logic change required.
- Inform operations team about expected increase in runtime only during retry scenarios.

Step 4: Escalate to Solution 2 only if required
- If failures continue across multiple SQL Server connections, apply platform/org-level setting.
- Plan Secure Agent/runtime restart during approved maintenance window.
```

```text
# Slide 5: Testing Limitation and Validation Approach

## Why Retry May Not Trigger in Basic Tests

Retry behavior should be validated using transient failures, not permanent configuration errors.

Invalid test cases:
- Wrong hostname
- Wrong port
- Wrong database name
- Wrong credentials

These are non-retryable failures and may fail immediately.

Valid retry scenarios:
- SQL Server temporarily unavailable
- Short network interruption
- SQL Server listener delay
- Port 1433 temporarily unreachable
- SQL Server slow response during connection

Lower environment validation:
- Confirm connection works after configuration.
- Confirm Secure Agent/runtime remains stable.
- If DBA/network team can simulate a transient DB issue, validate retry behavior.
- If not feasible, validate in production through monitoring after controlled implementation.
```

```text
# Slide 6: Post-Implementation Monitoring

## How Admins Can Confirm Retry Is Working

After implementing Solution 1, admins should monitor the affected jobs for retry behavior and runtime changes.

What to check:

| Monitoring Area | What to Look For |
|---|---|
| IICS Monitor | Job succeeds instead of failing on initial timeout |
| Runtime duration | Job runtime may increase by retry interval/time spent retrying |
| Session log | Timeout followed by successful reconnect/continued execution |
| Secure Agent logs | Retry/reconnect/driver-level connection messages |
| SQL Server logs | Multiple connection/login attempts from Secure Agent host |
| Failure trend | Reduction in timeout failures and manual reruns |

Success criteria:
- Fewer SQL Server timeout failures.
- Fewer manual job restarts.
- Jobs complete successfully when transient issue clears.
- No negative impact to unrelated tasks.
- Retry behavior is visible through monitor runtime/logs or Secure Agent logs.

Next step:
- Monitor for 1–2 weeks after Solution 1.
- If failures continue, proceed with Solution 2 platform/org-level configuration.
```
