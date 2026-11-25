-- @suite
-- @case
-- @desc: designed resource boundaries of collation-aware pattern matching raise bounded resource-limit errors; below-bound controls keep folded results (issue #31175, design r5 invariant 7)
-- @label:bvt
DROP DATABASE IF EXISTS issue_31175_limits;
CREATE DATABASE issue_31175_limits;
USE issue_31175_limits;

-- Boundary 1: regexp2-route expansion exhaustion at the fold per-pattern
-- emission budget. Sixteen distinct 80-s branches (distinct one-letter
-- suffixes defeat deduplication) emit about 1.1 MiB of faithful fold
-- expansion, crossing the rewrite's 1 MiB per-pattern emission budget
-- before engine admission; the operator raises the bounded resource-limit
-- error instead of trading down to per-character caseless matching.
-- @regex("regexp case-fold resource limit exceeded: pattern emission budget", true)
SELECT REGEXP_LIKE(REPEAT('s', 80), CONCAT(REPEAT('s', 80), 'a|', REPEAT('s', 80), 'b|', REPEAT('s', 80), 'c|', REPEAT('s', 80), 'd|', REPEAT('s', 80), 'e|', REPEAT('s', 80), 'f|', REPEAT('s', 80), 'g|', REPEAT('s', 80), 'h|', REPEAT('s', 80), 'i|', REPEAT('s', 80), 'j|', REPEAT('s', 80), 'k|', REPEAT('s', 80), 'l|', REPEAT('s', 80), 'm|', REPEAT('s', 80), 'n|', REPEAT('s', 80), 'o|', REPEAT('s', 80), 'p(?#x)'), 'i');
-- The case-sensitive control never rewrites and matches its own branch.
SELECT REGEXP_LIKE(CONCAT(REPEAT('s', 80), 'p'), CONCAT(REPEAT('s', 80), 'a|', REPEAT('s', 80), 'b|', REPEAT('s', 80), 'c|', REPEAT('s', 80), 'd|', REPEAT('s', 80), 'e|', REPEAT('s', 80), 'f|', REPEAT('s', 80), 'g|', REPEAT('s', 80), 'h|', REPEAT('s', 80), 'i|', REPEAT('s', 80), 'j|', REPEAT('s', 80), 'k|', REPEAT('s', 80), 'l|', REPEAT('s', 80), 'm|', REPEAT('s', 80), 'n|', REPEAT('s', 80), 'o|', REPEAT('s', 80), 'p(?#x)'), 'c');
-- Below every bound on the same route the expansion folds fully: two
-- distinct 40-s branches stay inside the emission budget and compile under
-- the 4,096-track bound, so twenty ß followed by 'a' match the first
-- branch (forty s then 'a'). The unsuffixed subject is the negative
-- control: both branches require a trailing letter it does not have.
SELECT REGEXP_LIKE(CONCAT(REPEAT('ß', 20), 'a'), CONCAT(REPEAT('s', 40), 'a|', REPEAT('s', 40), 'b(?#x)'), 'i');
SELECT REGEXP_LIKE(REPEAT('ß', 20), CONCAT(REPEAT('s', 40), 'a|', REPEAT('s', 40), 'b(?#x)'), 'i');

-- Boundary 2: RE2 height-bound exhaustion. 999 nested captures around the
-- foldable run 'ss' parse at exactly the Go regexp parse-tree height bound
-- unexpanded; the expansion's extra levels push it over, and the admitted
-- source pattern yields the bounded resource-limit error.
-- @regex("regexp case-fold resource limit exceeded: .*expression nests too deeply", true)
SELECT REGEXP_LIKE('ß', CONCAT(REPEAT('(', 999), 'ss', REPEAT(')', 999)), 'i');
-- Below the bound the same shape folds fully and matches through ß.
SELECT REGEXP_LIKE('ß', CONCAT(REPEAT('(', 997), 'ss', REPEAT(')', 997)), 'i');

-- Boundary 3: LIKE total-work-bound exhaustion (utf8mb4_unicode_ci
-- weighted matcher). The fragmented pattern '%' + 15,000 'a_' pairs + the
-- 92-distinct-weight alphabet interleaved with '_' + '%' is one
-- 30,184-wide segment whose 93 distinct weight vectors exceed the
-- Shift-And mask budget, forcing the one-character 'a' anchor fallback;
-- against 60,000 'a' characters every position is a candidate, and the
-- charged comparisons cross 4 x words x subject_chars = 113,280,000,
-- raising the bounded resource-limit error instead of running unbounded
-- verification work.
-- @regex("LIKE pattern resource limit exceeded: total anchor-search work bound", true)
SELECT REPEAT('a', 60000) COLLATE utf8mb4_unicode_ci LIKE CONCAT('%', REPEAT('a_', 15000), 'a_b_c_d_e_f_g_h_i_j_k_l_m_n_o_p_q_r_s_t_u_v_w_x_y_z_0_1_2_3_4_5_6_7_8_9_α_β_γ_δ_ε_ζ_η_θ_ι_κ_λ_μ_ν_ξ_ο_π_ρ_σ_τ_υ_φ_χ_ψ_ω_а_б_в_г_д_е_ж_з_и_й_к_л_м_н_о_п_р_с_т_у_ф_х_ц_ч_ш_щ_ъ_ы_ь_э_ю_я_', '%') COLLATE utf8mb4_unicode_ci;
-- Below the bound the exact occurrence matches at its first candidate and
-- an anchor-free subject reports no match, both with normal results.
SELECT CONCAT('q', REPEAT('aX', 15000), 'aXbXcXdXeXfXgXhXiXjXkXlXmXnXoXpXqXrXsXtXuXvXwXxXyXzX0X1X2X3X4X5X6X7X8X9XαXβXγXδXεXζXηXθXιXκXλXμXνXξXοXπXρXσXτXυXφXχXψXωXаXбXвXгXдXеXжXзXиXйXкXлXмXнXоXпXрXсXтXуXфXхXцXчXшXщXъXыXьXэXюXяX', 'z') COLLATE utf8mb4_unicode_ci LIKE CONCAT('%', REPEAT('a_', 15000), 'a_b_c_d_e_f_g_h_i_j_k_l_m_n_o_p_q_r_s_t_u_v_w_x_y_z_0_1_2_3_4_5_6_7_8_9_α_β_γ_δ_ε_ζ_η_θ_ι_κ_λ_μ_ν_ξ_ο_π_ρ_σ_τ_υ_φ_χ_ψ_ω_а_б_в_г_д_е_ж_з_и_й_к_л_м_н_о_п_р_с_т_у_ф_х_ц_ч_ш_щ_ъ_ы_ь_э_ю_я_', '%') COLLATE utf8mb4_unicode_ci;
SELECT REPEAT('b', 60000) COLLATE utf8mb4_unicode_ci LIKE CONCAT('%', REPEAT('a_', 15000), 'a_b_c_d_e_f_g_h_i_j_k_l_m_n_o_p_q_r_s_t_u_v_w_x_y_z_0_1_2_3_4_5_6_7_8_9_α_β_γ_δ_ε_ζ_η_θ_ι_κ_λ_μ_ν_ξ_ο_π_ρ_σ_τ_υ_φ_χ_ψ_ω_а_б_в_г_д_е_ж_з_и_й_к_л_м_н_о_п_р_с_т_у_ф_х_ц_ч_ш_щ_ъ_ы_ь_э_ю_я_', '%') COLLATE utf8mb4_unicode_ci;

-- Malformed sources keep their own invalid-input verdicts on both
-- match-types; the resource-limit error family never launders them.
-- (?#x) forces the regexp2 route.
SELECT REGEXP_LIKE('ss{', 'ss{(?#x)', 'i');
SELECT REGEXP_LIKE('ss{', 'ss{(?#x)', 'c');

DROP DATABASE issue_31175_limits;
