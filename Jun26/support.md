Below is a **solid working document / deck content** you can use for the Admin call. I have written it in a way that can be directly converted into **4–5 slides** using the Hilton template.

---

# IICS SQL Server Connectivity Timeout Failures – Retry & Timeout Configuration Proposal

## 1. Problem Description and Production Failure Pattern

We are observing intermittent failures in IICS jobs while connecting to SQL Server. The failures occur during the initial connection attempt from the IICS Secure Agent to the SQL Server database.

The session logs show timeout-related errors, such as **“Timeout expired”**, while the same task succeeds when it is manually restarted without any code, mapping, connection, or database-side configuration change.

This indicates that the issue is most likely caused by **transient connectivity or SQL Server availability issues**, rather than a permanent configuration issue such as an incorrect hostname, port, database name, or credentials. Informatica Support also confirmed that this behavior is consistent with transient connectivity issues between the Secure Agent and the SQL Server endpoint, and suggested increasing login/connection timeout and enabling automatic connection retries using the referenced Informatica KB article. Informatica’s KB result also describes configuration through **Administrator → Runtime Environments → Secure Agent**, which indicates the change is applied at Secure Agent / runtime environment level rather than inside an individual mapping. ([Informatica Knowledge][1])

### Production examples / reference

| Item                   | Details                                                                                               |
| ---------------------- | ----------------------------------------------------------------------------------------------------- |
| Application            | IICS / Cloud Data Integration                                                                         |
| Database               | Microsoft SQL Server                                                                                  |
| Failure type           | Intermittent connection timeout                                                                       |
| Observed behavior      | Job fails initially, succeeds on manual rerun                                                         |
| Example task           | `INFAPD_PPEPM_CMD_wf_m_PSFIN_TO_PROPPLAN_ACT_BS_CP`                                                   |
| Support case           | Informatica GCS Case `04996054`                                                                       |
| Support summary        | SQL Server Connectivity Issues in IICS – Request for Retry Mechanism & Timeout Configuration Guidance |
| Support recommendation | Increase login/connection timeout and enable automatic connection retries                             |
| Constraint             | Some settings may require Secure Agent / runtime restart, affecting other projects on same runtime    |

---

## 2. Solution 1 – Connection / Secure Agent Level Retry and Timeout Configuration

### Objective

Implement Informatica-recommended retry and timeout configuration so that transient SQL Server connection failures are retried automatically before the IICS job is marked as failed.

This solution is intended to address cases where:

| Scenario                                       | Expected handling                          |
| ---------------------------------------------- | ------------------------------------------ |
| SQL Server is temporarily unavailable          | IICS should retry connection               |
| Network path has a short interruption          | IICS should retry connection               |
| SQL Server listener/port is slow to respond    | IICS should wait longer before failing     |
| Database becomes available after a short delay | Job should continue without manual restart |

This solution is **not expected** to fix permanent/non-retryable failures such as:

| Non-retryable scenario  | Why retry should not help         |
| ----------------------- | --------------------------------- |
| Wrong hostname          | Endpoint is invalid               |
| Wrong port              | Connection route is invalid       |
| Wrong database name     | Configuration issue               |
| Wrong username/password | Authentication failure            |
| User permission issue   | Authorization/configuration issue |

This is important because our initial test using incorrect host/database/port did not trigger retry behavior, which is expected because these are not valid transient failures.

### Configuration Parameters to Discuss

Since the exact property names and acceptable values must be confirmed against the Informatica KB and the SQL Server connector/runtime version, we should present the parameters functionally in the Admin call.

| Parameter area             | Purpose                                                                                                              | Current value |                             Recommended value | Impact                                                 |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------: | --------------------------------------------: | ------------------------------------------------------ |
| Login / connection timeout | Controls how long IICS waits while establishing SQL Server connection before failing                                 |           TBD | Increase from default, example 60–120 seconds | Reduces false failures when SQL Server/network is slow |
| Retry enablement           | Enables automatic retry for eligible transient connection failures                                                   |           TBD |                                       Enabled | Allows IICS to retry instead of failing immediately    |
| Retry count                | Number of retry attempts before task fails                                                                           |           TBD |                            Example 3 attempts | Gives transient issues time to recover                 |
| Retry interval / delay     | Wait time between retry attempts                                                                                     |           TBD |                         Example 30–60 seconds | Avoids immediate repeated failures                     |
| Scope of setting           | Determines whether setting applies to one connection, SQL Server connector, Secure Agent, or all database connectors |           TBD |               Prefer narrowest scope possible | Reduces impact to other projects                       |

### Recommended starting values

For discussion with Admins and Informatica:

| Setting                  | Proposed initial value |
| ------------------------ | ---------------------: |
| Connection/login timeout |             60 seconds |
| Retry count              |                      3 |
| Retry interval           |             30 seconds |
| Total retry window       |     Around 2–3 minutes |

This is a safe starting point because it avoids making the job wait too long, while still allowing short-lived SQL Server/network interruptions to recover.

---

## 3. Solution 2a – Platform Level Configuration for SQL Server Only

If Solution 1 cannot be applied at a specific connection level, the next preferred approach is to apply the retry/timeout configuration at the **platform/runtime level for SQL Server connections only**.

### Why this option is useful

This provides retry protection for all SQL Server-based IICS jobs running on the affected Secure Agent or runtime environment.

| Benefit                                     | Explanation                                                                |
| ------------------------------------------- | -------------------------------------------------------------------------- |
| Covers all SQL Server jobs                  | Helps multiple workflows impacted by similar SQL Server timeout failures   |
| Lower operational effort                    | One configuration instead of changing multiple tasks/connections           |
| Aligned with root cause                     | Issue is related to SQL Server connectivity from Secure Agent              |
| Lower blast radius than all-database change | Does not affect Oracle, Snowflake, DB2, etc., if scoped only to SQL Server |

### Risk / consideration

| Risk                                               | Mitigation                              |
| -------------------------------------------------- | --------------------------------------- |
| Requires Secure Agent / service restart            | Plan downtime window                    |
| May affect all SQL Server projects on same runtime | Notify dependent teams                  |
| Longer failure detection time                      | Keep retry count and timeout controlled |
| Retry could mask recurring infrastructure issue    | Add monitoring and reporting            |

### Recommendation

Use Solution 2a if Informatica confirms that SQL Server-specific retry properties can be applied at runtime/Secure Agent level without affecting all database connectors.

---

## 4. Solution 2b – Platform Level Configuration for All Databases

This is the broadest option and should be considered only if SQL Server-specific configuration is not available.

### Description

Apply timeout/retry configuration at the broader platform/runtime level so that retry behavior is available for database connectivity failures across multiple database technologies.

### Benefits

| Benefit                                     | Explanation                                                        |
| ------------------------------------------- | ------------------------------------------------------------------ |
| Consistent retry behavior                   | All DB jobs get protection against transient connectivity failures |
| Easier central administration               | One runtime-level setting                                          |
| Useful if similar failures occur across DBs | Helps if issue is common network/Secure Agent connectivity         |

### Risks

| Risk                                    | Explanation                                                     |
| --------------------------------------- | --------------------------------------------------------------- |
| High blast radius                       | All projects using the runtime may be affected                  |
| Unexpected behavior for other databases | Different DB connectors may interpret timeout/retry differently |
| Increased wait time before failure      | Genuine failures may take longer to surface                     |
| Requires broader approval               | Admin, DBA, project teams, and operations may need sign-off     |

### Recommendation

Use Solution 2b only as a fallback after confirming that connection-level or SQL Server-specific configuration is not possible.

---

## 5. Why Solution 1 Cannot Be Fully Tested in Lower Environment but Still Needs Production Implementation

The challenge with testing is that retry behavior only occurs for **eligible transient failures**.

When we test using wrong hostname, wrong database name, wrong port, or incorrect credentials, Informatica treats these as non-retryable configuration/authentication failures. Therefore, retry may not trigger, and this does not prove that the retry configuration is ineffective.

To properly validate retry behavior, we need a realistic transient failure simulation, such as:

| Test scenario                                                  | Required team           |
| -------------------------------------------------------------- | ----------------------- |
| Temporarily stop SQL Server service during connection attempt  | DBA                     |
| Temporarily block SQL Server port 1433 and restore it          | Network / firewall team |
| Simulate network drop between Secure Agent host and SQL Server | Network team            |
| Delay SQL Server response temporarily                          | DBA / infra team        |
| Restart SQL Server listener during job start                   | DBA                     |

Without this controlled simulation, lower-environment testing may only confirm that the Secure Agent starts successfully with the new settings, but not that retry is functionally triggered.

Therefore, if transient DB/network issue simulation is not feasible in test, the practical approach is:

1. Implement Solution 1 in production during approved downtime.
2. Monitor the previously failing jobs.
3. Confirm whether timeout failures are reduced.
4. Review IICS session logs and Secure Agent logs for retry evidence.
5. Escalate to Informatica if retry attempts are not visible.

---

## 6. Monitoring Plan After Solution 1 Implementation

Once Solution 1 is implemented in production, Admins should monitor both IICS job status and Secure Agent logs.

### What to monitor

| Area              | What to check                                                          |
| ----------------- | ---------------------------------------------------------------------- |
| IICS Monitor      | Whether jobs complete successfully instead of failing on first timeout |
| Session logs      | Any retry-related messages before successful connection                |
| Secure Agent logs | Retry attempt, timeout, reconnect, or driver-level messages            |
| SQL Server logs   | Login attempts from Secure Agent host                                  |
| Network logs      | Firewall drops, routing issues, port availability                      |
| Failure trend     | Compare failures before and after configuration change                 |

### Success criteria

| Criteria          | Expected result                                             |
| ----------------- | ----------------------------------------------------------- |
| Job failure count | Reduced timeout-related failures                            |
| Manual reruns     | Reduced need for manual restart                             |
| Job duration      | Slight increase only when retry occurs                      |
| Data consistency  | No duplicate or partial processing introduced               |
| Logs              | Evidence of retry attempts or delayed successful connection |

### How to confirm Solution 1 is working before moving to Solution 2

We should not immediately move to Solution 2. First, monitor Solution 1 for a defined period.

Recommended validation window:

| Item                | Recommendation                                   |
| ------------------- | ------------------------------------------------ |
| Monitoring duration | 1–2 weeks or minimum 10–15 production runs       |
| Compare against     | Previous timeout failure history                 |
| Confirm with        | IICS Monitor, Secure Agent logs, SQL Server logs |
| Decision point      | If failures continue, proceed to Solution 2a     |

---

## 7. Questions to Ask Informatica / Admins

These are the most important questions to take into the call.

### Retry behavior and logging

1. When retry is enabled for SQL Server connectivity timeout, where exactly will retry attempts be logged?

   * IICS Monitor?
   * Session log?
   * Secure Agent log?
   * Connector/driver log?

2. What exact log message confirms that IICS retried the SQL Server connection?

3. Does IICS retry only during initial connection, or also during mid-session connection loss?

4. Which SQL Server errors are considered retryable?

   * Login timeout?
   * TCP timeout?
   * Network error?
   * SQL Server unavailable?
   * Deadlock?
   * Query timeout?

5. Which errors are non-retryable?

### Configuration scope

6. Can the retry/timeout setting be applied to only one SQL Server connection?

7. If not connection-level, can it be applied only to Microsoft SQL Server connector?

8. If it is Secure Agent/runtime-level, will it affect all projects using that runtime?

9. Does enabling this require restarting:

   * Secure Agent only?
   * Data Integration Server service?
   * All Secure Agent services?
   * Entire runtime environment?

10. Is there downtime impact for currently running jobs?

### Recommended values

11. What values does Informatica recommend for:

* login timeout
* retry count
* retry interval
* total retry window

12. Are there any upper limits or known side effects?

13. Will increasing timeout/retry impact task SLA or concurrency?

### Testing

14. How can we safely simulate a retryable SQL Server connectivity failure in lower environment?

15. Does Informatica have a recommended test procedure for validating retry behavior?

16. Is wrong host/port/credential expected to bypass retry because it is non-retryable?

### Production implementation

17. Can Informatica confirm that this approach is suitable for our support case `04996054`?

18. Can Informatica provide official confirmation that Solution 1 is recommended before moving to platform-wide Solution 2?

---

## 8. Proposed Decision Path

| Priority | Option                                                                 | Recommendation                              |
| -------: | ---------------------------------------------------------------------- | ------------------------------------------- |
|        1 | Solution 1 – connection / narrow Secure Agent SQL Server retry setting | Preferred                                   |
|        2 | Solution 2a – platform level for SQL Server only                       | Use if Solution 1 cannot be scoped narrowly |
|        3 | Solution 2b – platform level for all databases                         | Last option only                            |

### Final recommendation

Proceed with Solution 1 first, with Informatica-confirmed parameter names and values. Because lower-environment validation requires transient DB/network simulation, testing with wrong connection details should not be treated as a valid retry test. After production implementation, monitor IICS session logs, Secure Agent logs, SQL Server login attempts, and job failure trends before deciding whether Solution 2a or 2b is required.

---

# Suggested 5-Slide Deck Structure

## Slide 1 – Problem Statement & Production Impact

**Title:** Intermittent SQL Server Connectivity Timeout Failures in IICS

**Content:**

* IICS jobs intermittently fail while connecting to SQL Server.
* Logs show timeout-related errors.
* Same job succeeds on manual rerun without changes.
* Indicates transient DB/network/Secure Agent-to-SQL connectivity issue.
* Informatica Support Case: `04996054`.
* Ask: enable automatic retry and timeout handling to reduce manual reruns.

---

## Slide 2 – Solution 1: Connection / Secure Agent Level Retry Configuration

**Title:** Preferred Solution – Enable SQL Server Retry and Timeout Handling

**Content:**

| Parameter                | Purpose                               | Current |        Recommended |
| ------------------------ | ------------------------------------- | ------: | -----------------: |
| Login/connection timeout | Wait longer before failing connection |     TBD |         60–120 sec |
| Retry enablement         | Retry transient connection failure    |     TBD |             Enable |
| Retry count              | Number of retry attempts              |     TBD |                  3 |
| Retry interval           | Delay between attempts                |     TBD |          30–60 sec |
| Scope                    | Limit impact                          |     TBD | Narrowest possible |

**Key point:** Wrong host/port/credentials are non-retryable and should not be used to validate retry behavior.

---

## Slide 3 – Solution 2a and 2b: Platform-Level Alternatives

**Title:** Alternative Options if Solution 1 Cannot Be Scoped

**Content:**

| Option      | Scope           | Pros                       | Risk                                        |
| ----------- | --------------- | -------------------------- | ------------------------------------------- |
| Solution 2a | SQL Server only | Covers all SQL Server jobs | Impacts all SQL Server workloads on runtime |
| Solution 2b | All DBs         | Central retry behavior     | Highest blast radius                        |

**Recommendation:** Prefer Solution 2a over Solution 2b if platform-level change is required.

---

## Slide 4 – Testing Limitation and Production Validation Plan

**Title:** Why Lower Environment Testing May Not Fully Validate Retry

**Content:**

* Retry requires a transient failure.
* Wrong host, DB name, port, or credentials are permanent failures.
* Proper testing needs DBA/network support.
* Valid simulations:

  * SQL Server temporary downtime
  * Port 1433 temporary block
  * Network interruption
  * SQL listener restart
* If simulation is not feasible, implement in production during approved window and monitor.

---

## Slide 5 – Monitoring, Success Criteria, and Questions for Informatica

**Title:** Post-Implementation Monitoring and Confirmation Required

**Content:**

| Monitor           | Expected evidence                              |
| ----------------- | ---------------------------------------------- |
| IICS Monitor      | Fewer timeout job failures                     |
| Session logs      | Retry or reconnect evidence                    |
| Secure Agent logs | Driver/connection retry messages               |
| SQL Server logs   | Multiple login attempts / successful reconnect |
| Trend report      | Reduction in manual reruns                     |

**Questions for Informatica:**

* Where is retry logged?
* What exact message confirms retry?
* Which SQL Server errors are retryable?
* Does this require full Secure Agent/runtime restart?
* Can the setting be limited to SQL Server only?

---

# Questions I Need From You

1. What is the exact SQL Server connection type in IICS — JDBC, ODBC, or native SQL Server connector?
2. Do you have the exact timeout error from the session log?
3. What is the current Secure Agent runtime name?
4. Are multiple projects using the same Secure Agent?
5. Do you know the exact parameters Informatica suggested from the KB article?
6. Is the failure happening at source connection, target connection, lookup connection, or stored procedure connection?
7. How many failures happened in production recently, and over what period?
8. Do you want this as a **formal Word document** or a **4–5 slide PPT deck** in the Hilton template?

[1]: https://knowledge.informatica.com/s/article/561890?language=en_US&utm_source=chatgpt.com "Is it possible to set additional connection properties for a ..."
