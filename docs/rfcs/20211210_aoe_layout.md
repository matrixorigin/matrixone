- Feature Name: Analytic Optimized Engine Layout
- Status: In Progress
- Start Date: 2021-05-10
- Authors: [Xu Peng](https://github.com/XuPeng-SH)
- Implementation PR: [#1335](https://github.com/matrixorigin/matrixone/pull/1335)
- Issue for this RFC:

# Summary
This is a proposal to define the data persistent and in-memory layout.

# Motivation
**AOE** (Analytic Optimized Engine) is designed for analytical query workloads. In practice, a columnar store is well-suited for OLAP-like worloads.

# Detailed Design
## Hirachy
![image](https://user-images.githubusercontent.com/39627130/145529173-1c6ad8eb-84e2-4d7e-a49a-9085153f3436.png)

As described in [Here](https://github.com/matrixorigin/matrixone/blob/main/docs/rfcs/20211210_aoe_overall_design.md#data-storage)
