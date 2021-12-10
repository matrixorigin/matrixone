- Feature Name: Analytic Optimized Engine Layout
- Status: In Progress
- Start Date: 2021-05-10
- Authors: [Xu Peng](https://github.com/XuPeng-SH)
- Implementation PR:
- Issue for this RFC:

# Summary
This is a proposal to define the data persistent and in-memory layout.

# Motivation
**AOE** (Analytic Optimized Engine) is designed for analytical query workloads. In practice, a columnar store is well-suited for OLAP-like worloads.

# Detailed Design
## Data Hirachy
![image](https://user-images.githubusercontent.com/39627130/145529173-1c6ad8eb-84e2-4d7e-a49a-9085153f3436.png)

As described [Here](https://github.com/matrixorigin/matrixone/blob/main/docs/rfcs/20211210_aoe_overall_design.md#data-storage). Each table data is a three-level LSM tree. For example, the maximum number of blocks in a segment is 4. Segment [0,1,2] are already merge sorted, respectively corresponding to a sort run in **L2**. Segment [3] is not merge sorted, but Block [12,13] have been sorted, respectively corresponding to a sort run in **L1**. Transient block 14 has reached the maximum row count of a block and is flowing to **L1**. Transient block 15 is the latest appendable block.
![image](https://user-images.githubusercontent.com/39627130/145538157-1cd4bd28-d9a3-42fc-8879-f7b4e19c96da.png)

## File Format
![image](https://user-images.githubusercontent.com/39627130/145574992-9240f59a-2713-4aa5-93d7-07d2b9fc1ed4.png)
- Zones
  1) Header
  2) Footer
  3) MetaInfo
  4) Columns
  5) Indexes
- Indexes that specified in **CREATE TABLE** statement will be embedded in the segment file. Otherwise, there is a dedicate index file for the specified index.
- Zonemap index is automatically created for all columns and embedded in the segment file.
- In a segment or block file, the data of each column is stored in a continuous space.
- Column block
  1) Fixed-length type column block format (Uncompressed)
     ![image](https://user-images.githubusercontent.com/39627130/145585444-692d7c2c-e884-4a2d-a1ca-59ce9da0230b.png)
  2) Variable-length type column block format (Uncompressed)
     ![image](https://user-images.githubusercontent.com/39627130/145585482-b58c2baf-adec-4cee-b03c-faadefbbce54.png)

## Compression
The compression unit is always a column block. The compression algo, compressed and uncompressed size are all serialized into **MetaInfo** zone.
