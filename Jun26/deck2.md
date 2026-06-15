Got it. Below is a **balanced version** — not too detailed, not too brief. This should fit well into **5–6 slides** with summary-style points.

---

# Slide 1: Problem Statement & Production Failure Pattern

## Intermittent SQL Server Timeout Failures in IICS

### Summary

We are observing repeated intermittent failures in IICS jobs while connecting to SQL Server. The jobs fail with a timeout error, but when the same job is manually restarted, it succeeds without any configuration or code change.

### Key Observations

* Failure occurs during SQL Server connectivity from IICS Secure Agent.
* Logs indicate timeout-related errors, such as **“Timeout expired.”**
* Same job succeeds on manual rerun.
* No change is made to mapping, task, connection, credentials, or database object.
* This indicates a transient connectivity issue rather than a permanent configuration issue.

### Production Reference

| Item         | Details                                             |
| ------------ | --------------------------------------------------- |
| Platform     | IICS / Cloud Data Integration                       |
| Database     | Microsoft SQL Server                                |
| Failure type | Connection timeout                                  |
| Example job  | `INFAPD_PPEPM_CMD_wf_m_PSFIN_TO_PROPPLAN_ACT_BS_CP` |
| Support case | Informatica GCS Case `04996054`                     |

### Impact

* Manual monitoring and rerun required.
* Delay in downstream processing.
* Repeated operational effort.
* Risk of missing SLA if failures are not noticed immediately.

---

# Slide 2: Informatica Support Recommendation

## Retry and Timeout Configuration Guidance

### Summary

Informatica Support reviewed the issue and confirmed that the behavior is consistent with transient connectivity issues between the IICS Secure Agent and the SQL Server endpoint.

Support suggested increasing the login/connection timeout and enabling retry-related configuration using the Informatica KB article shared for Microsoft SQL Server connection additional properties.

### Support Understanding

* Job fails when Secure Agent first tries to connect to SQL Server.
* SQL Server driver returns a timeout error.
* Since rerun succeeds without changes, it is likely not a permanent configuration issue.
* Retry/timeout configuration may help reduce such intermittent failures.

### Important Clarification

Testing retry using incorrect host, port, database name, or credentials is not a valid retry test.

Those are generally treated as permanent/non-retryable failures.

| Test scenario                      | Retry expected? | Reason                   |
| ---------------------------------- | --------------- | ------------------------ |
| Wrong hostname                     | No              | Invalid endpoint         |
| Wrong port                         | No              | Invalid network route    |
| Wrong credentials                  | No              | Authentication failure   |
| SQL Server temporarily unavailable | Yes             | Transient failure        |
| Short network interruption         | Yes             | Transient failure        |
| SQL Server slow to respond         | Yes             | Timeout/retry applicable |

---

# Slide 3: Solution 1 – Connection / Runtime Level Retry Configuration

## Preferred Solution: Enable SQL Server Retry Handling

### Summary

The preferred approach is to apply retry and timeout settings at the narrowest possible scope, ideally at the SQL Server connection or SQL Server-specific Secure Agent/runtime level.

This allows IICS to retry eligible SQL Server connection timeout failures before marking the job as failed.

### Expected Behavior

1. IICS job starts.
2. Secure Agent attempts SQL Server connection.
3. First connection attempt times out.
4. IICS waits for configured retry interval.
5. IICS retries the connection.
6. If SQL Server becomes reachable, the job continues.
7. If all retries fail, the job fails normally.

### Parameters to Confirm

| Parameter                | Purpose                                   | Current Value |  Recommended Value |
| ------------------------ | ----------------------------------------- | ------------: | -----------------: |
| Connection/Login Timeout | Wait time before connection attempt fails |           TBD |         60–120 sec |
| Retry Enablement         | Enables retry for eligible failures       |           TBD |            Enabled |
| Retry Count              | Number of retry attempts                  |           TBD |                  3 |
| Retry Interval           | Delay between retry attempts              |           TBD |          30–60 sec |
| Scope                    | Where setting applies                     |           TBD | Narrowest possible |

### Recommendation

Start with moderate values such as:

* Timeout: **60 seconds**
* Retry count: **3**
* Retry interval: **30 seconds**

This gives a total retry window of around **2–3 minutes**, which is reasonable for transient SQL Server/network issues.

---

# Slide 4: Solution 2a and 2b – Platform Level Alternatives

## Alternative Options if Solution 1 Cannot Be Scoped

### Summary

If retry configuration cannot be applied at connection level, then platform-level options need to be considered.

The main concern is the scope of impact because platform-level changes may affect multiple projects using the same Secure Agent/runtime environment.

### Solution 2a: Platform Level for SQL Server

Apply retry/timeout configuration only for SQL Server connections at the runtime/Secure Agent level.

| Benefit                                     | Risk                                             |
| ------------------------------------------- | ------------------------------------------------ |
| Covers all SQL Server jobs                  | Impacts all SQL Server workloads on that runtime |
| Useful if many SQL jobs face same issue     | Requires runtime/Secure Agent restart            |
| Lower blast radius than all-database change | Needs coordination with affected teams           |

### Solution 2b: Platform Level for All Databases

Apply retry/timeout configuration broadly across database connectors.

| Benefit                                       | Risk                                   |
| --------------------------------------------- | -------------------------------------- |
| Central retry behavior for all DB connections | Highest impact scope                   |
| Easier to manage centrally                    | May affect unrelated projects          |
| Useful if issue is not SQL Server-specific    | Longer failure time for genuine errors |

### Recommendation

| Priority | Option      | Recommendation                                |
| -------: | ----------- | --------------------------------------------- |
|        1 | Solution 1  | Preferred                                     |
|        2 | Solution 2a | Use if connection-level scope is not possible |
|        3 | Solution 2b | Last option only                              |

---

# Slide 5: Testing Limitation and Why Production Implementation May Be Needed

## Retry Validation Requires Real Transient Failure Simulation

### Summary

Retry behavior cannot be fully validated by giving wrong connection details. Incorrect hostname, port, database, or credentials are permanent failures, so retry may not trigger.

To properly test retry, we need to recreate a temporary SQL Server or network issue.

### Valid Retry Test Scenarios

| Scenario                                     | Required Support      |
| -------------------------------------------- | --------------------- |
| Temporarily stop SQL Server service          | DBA team              |
| Temporarily block SQL Server port 1433       | Network/firewall team |
| Simulate short network drop                  | Network team          |
| Restart SQL Server listener during job start | DBA team              |
| Delay SQL Server response                    | DBA / Infra team      |

### Challenge

In lower environments, it may not be easy to simulate the exact production issue safely.

So testing may only confirm that:

* Secure Agent comes back successfully after configuration.
* Jobs continue to run after restart.
* No immediate negative impact is seen.

But it may not prove retry behavior unless a valid transient failure is created.

### Proposed Approach

* Apply configuration first in lower environment.
* Validate Secure Agent restart and normal job execution.
* If transient issue cannot be simulated, proceed with controlled production implementation.
* Monitor production runs after implementation.

---

# Slide 6: Monitoring Plan and Questions for Informatica/Admins

## Post-Implementation Validation

### Summary

After Solution 1 is implemented, we need to monitor job behavior and confirm whether retry is happening as expected.

### What to Monitor

| Area              | What to Check                             |
| ----------------- | ----------------------------------------- |
| IICS Monitor      | Whether timeout failures reduce           |
| Session logs      | Retry/reconnect messages                  |
| Secure Agent logs | Driver-level connection retry evidence    |
| SQL Server logs   | Multiple login attempts from Secure Agent |
| Failure trend     | Compare before vs after implementation    |
| Manual reruns     | Check whether manual restarts reduce      |

### Success Criteria

* Reduced SQL Server timeout failures.
* Reduced manual reruns.
* Jobs complete successfully after transient timeout.
* No negative impact to unrelated jobs.
* Retry evidence is available in logs or monitoring.

### Key Questions for Informatica

* Where exactly will retry attempts be logged?
* What log message confirms that retry happened?
* Which SQL Server errors are retryable?
* Does retry apply only during initial connection or also during mid-session connection loss?
* Can this setting be applied only to one SQL Server connection?
* If not, can it be limited to SQL Server only?
* Does the change require Secure Agent restart or full runtime restart?
* Will running jobs be impacted during restart?
* What values does Informatica recommend for timeout, retry count, and retry interval?
* Can Informatica confirm that wrong host/port/credentials are non-retryable test cases?

---

# Suggested Closing / Decision Slide Note

## Final Recommendation

Proceed with **Solution 1** first using Informatica-confirmed retry and timeout settings.

If connection-level scope is not possible, use **Solution 2a** for SQL Server-level configuration.

Use **Solution 2b** only if no SQL Server-specific option is available, because it has the widest impact.

The key dependency is Informatica confirmation on:

* exact parameters,
* scope of change,
* restart requirement,
* retryable error types,
* and where retry evidence will be logged.
