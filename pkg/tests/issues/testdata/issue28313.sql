WITH tagged AS (
WITH RECURSIVE acct_tree AS (
  SELECT cpmb_kgd4b76 AS node_id, parenth1 AS parent_id, 0 AS lvl
  FROM dwd_dcp.dwd_bw_1cpmb_bkgd4b76
  WHERE cpmb_kgd4b76 = '200000'
  UNION ALL
  SELECT c.cpmb_kgd4b76, c.parenth1, t.lvl + 1
  FROM dwd_dcp.dwd_bw_1cpmb_bkgd4b76 c
  INNER JOIN acct_tree t ON c.parenth1 = t.node_id
  WHERE t.lvl < 3
)
SELECT
  CONCAT(b.belnr, '-', b.bukrs, '-', b.gjahr) AS voucher_header_id,
  b.belnr, b.bukrs, b.gjahr, b.bldat,
  s.buzei, s.hkont, s.dmbtr, s.zuonr,
  t.node_id AS acct_node, t.lvl,
  pd.payment_type
FROM dwd_dcp.dwd_s4_bkpf b
INNER JOIN dwd_dcp.dwd_s4_bseg s ON b.bukrs = s.bukrs AND b.belnr = s.belnr AND b.gjahr = s.gjahr
INNER JOIN acct_tree t ON s.hkont = t.node_id
LEFT JOIN jst_receipts_tables.payment_details pd
  ON pd.belnr = b.belnr AND pd.bukrs = b.bukrs AND pd.gjahr = b.gjahr
WHERE b.gjahr IN ('2022', '2023', '2024', '2025') AND s.dmbtr <> 0
  AND b.bukrs IN ('1000', '1100', '1200', '1300', '2000', '2100', '2200', '3000')
)
SELECT /* jinpan_join_0261 */ * FROM tagged
LIMIT 500