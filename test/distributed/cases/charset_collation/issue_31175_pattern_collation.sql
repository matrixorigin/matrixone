-- @suite
-- @case
-- @desc: LIKE/REGEXP/REGEXP_LIKE/FIND_IN_SET honor the effective collation on literals and columns (issue #31175, #31179)
-- @label:bvt
DROP DATABASE IF EXISTS issue_31175;
CREATE DATABASE issue_31175;
USE issue_31175;

-- Default utf8mb4_general_ci literals: pattern operators agree with the
-- already-correct substring functions.
SELECT COLLATION('abc');
SELECT 'a' LIKE 'A', 'ABC' LIKE 'abc';
SELECT 'abc' REGEXP 'ABC', 'abc' REGEXP '[A-Z]+';
SELECT LOCATE('A', 'abc'), INSTR('abc', 'A'), POSITION('B' IN 'abc');
SELECT FIND_IN_SET('A', 'a,b'), FIND_IN_SET('B', 'a,b'), FIND_IN_SET('z', 'a,b');
SELECT 'a' NOT LIKE 'A', 'abc' NOT REGEXP 'ABC';

-- The sigma family (U+03A3/U+03C3/U+03C2) shares one fold orbit under a ci
-- collation, exactly as LOCATE/INSTR/POSITION and MySQL's shared weight treat
-- them; constant, varying, multi-% and escaped patterns all fold upward.
SELECT 'ς' LIKE 'Σ', 'σ' LIKE 'ς', 'Σ' LIKE 'σ';
SELECT 'ς' LIKE 'Σ%', 'xσ' LIKE 'Σ%', 'σxς' LIKE '%Σ%';
SELECT 'σ_ς' LIKE 'Σ!_Σ' ESCAPE '!', 'σxς' LIKE 'Σ!_Σ' ESCAPE '!';
SELECT 'ς' REGEXP 'Σ', 'σ' REGEXP 'ς';
SELECT FIND_IN_SET('ς', 'σ,Σ'), LOCATE('ς', 'σΣ');

-- Default-collation column predicates: the reported table case.
CREATE TABLE ci1 (name VARCHAR(30));
INSERT INTO ci1 VALUES ('Alice'), ('BOB'), ('carol'), ('DAVE');
SELECT COUNT(*) FROM ci1 WHERE name LIKE 'alice%';
SELECT COUNT(*) FROM ci1 WHERE name REGEXP '^alice';
SELECT name FROM ci1 WHERE name LIKE 'ALICE%' ORDER BY name;
SELECT name FROM ci1 WHERE name NOT LIKE 'alice%' ORDER BY name;
SELECT name FROM ci1 WHERE name REGEXP '^[a-c]' ORDER BY name;

-- Explicit utf8mb4_unicode_ci column: equality/IN already honored it; LIKE and
-- REGEXP must agree on the same case behavior.
CREATE TABLE uci2 (s VARCHAR(20) COLLATE utf8mb4_unicode_ci);
INSERT INTO uci2 VALUES ('Alice'), ('BOB');
SELECT COUNT(*) FROM uci2 WHERE s = 'alice';
SELECT COUNT(*) FROM uci2 WHERE s IN ('alice');
SELECT COUNT(*) FROM uci2 WHERE s LIKE 'alice%';
SELECT COUNT(*) FROM uci2 WHERE s LIKE 'ALICE%';
SELECT COUNT(*) FROM uci2 WHERE s REGEXP '^alice';
SELECT s FROM uci2 WHERE s LIKE 'bob' ORDER BY s;

-- Expression-level COLLATE overrides in either operand position.
SELECT 'Apple' = 'apple' COLLATE utf8mb4_unicode_ci;
SELECT 'Apple' LIKE 'apple' COLLATE utf8mb4_unicode_ci;
SELECT 'Apple' REGEXP 'apple' COLLATE utf8mb4_unicode_ci;
SELECT 'Apple' LIKE 'apple%' COLLATE utf8mb4_unicode_ci;
SELECT 'a' LIKE 'A' COLLATE utf8mb4_bin;
SELECT 'abc' REGEXP 'ABC' COLLATE utf8mb4_bin;

-- REGEXP_LIKE: collation supplies the default; explicit match_type wins.
SELECT REGEXP_LIKE('abc', 'ABC');
SELECT REGEXP_LIKE('abc', 'ABC', 'c');
SELECT REGEXP_LIKE('abc', 'ABC', 'i');
SELECT REGEXP_LIKE('ABC', '^abc', 'm');

-- Binary/case-sensitive controls keep the byte matcher.
SELECT _binary 'a' LIKE _binary 'A', _binary 'abc' REGEXP _binary 'ABC';
SELECT CAST('a' AS BINARY) LIKE CAST('A' AS BINARY);

-- LIKE resolves its comparison collation from ALL its arguments, ESCAPE
-- included (MySQL item_cmpfunc.cc aggregates every LIKE argument), so an
-- explicit utf8mb4_bin COLLATE on ESCAPE forces byte comparison while an
-- explicit ci COLLATE keeps the fold. REGEXP_LIKE's match_type never
-- participates (MySQL item_regexp_func.cc resolves the subject and pattern
-- only), so a COLLATE there can neither conflict nor force byte comparison.
SELECT 'a' LIKE 'A' ESCAPE ('!' COLLATE utf8mb4_bin);
SELECT 'a' LIKE 'A' ESCAPE ('!' COLLATE utf8mb4_general_ci);
SELECT REGEXP_LIKE('a' COLLATE utf8mb4_unicode_ci, 'A', 'i' COLLATE utf8mb4_bin);
SELECT REGEXP_LIKE('a' COLLATE utf8mb4_unicode_ci, 'A', 'm' COLLATE utf8mb4_bin);

-- A _binary introducer mixed with an explicit text collation keeps MySQL's
-- error 3995 rejection for REGEXP, in either operand order.
SELECT _binary 'abc' REGEXP 'ABC';
SELECT _binary 'abc' REGEXP ('ABC' COLLATE utf8mb4_general_ci);
SELECT ('ABC' COLLATE utf8mb4_unicode_ci) REGEXP _binary 'abc';
SELECT REGEXP_LIKE(_binary 'abc', 'ABC' COLLATE utf8mb4_general_ci);

-- LIKE and FIND_IN_SET honor the resolved comparison collation: an explicit
-- COLLATE wins coercibility aggregation over a binary string operand in
-- either operand order (values probed on a MySQL 8.0.44 oracle), so the
-- converted binary value is compared under that collation. With no explicit
-- COLLATE the binary operand keeps the byte-wise test, an explicit
-- utf8mb4_bin keeps it too, and an equal-coercibility tie between a text
-- literal and a binary literal stays byte-wise in either operand order.
SELECT _binary 'abc' LIKE ('ABC%' COLLATE utf8mb4_general_ci);
SELECT ('abc' COLLATE utf8mb4_general_ci) LIKE _binary 'ABC%';
SELECT _binary 'abc' LIKE ('ABC%' COLLATE utf8mb4_unicode_ci);
SELECT ('abc' COLLATE utf8mb4_unicode_ci) LIKE _binary 'ABC%';
SELECT _binary 'abc' LIKE 'ABC%', _binary 'abc' LIKE ('ABC%' COLLATE utf8mb4_bin);
SELECT 'abc' LIKE _binary 'ABC%';
SELECT _binary 'abc' LIKE ('abc%' COLLATE utf8mb4_general_ci), _binary 'xbc' LIKE ('ABC%' COLLATE utf8mb4_general_ci);
SELECT FIND_IN_SET(_binary 'A', 'a,b'), FIND_IN_SET('A', _binary 'a,b');
SELECT FIND_IN_SET(_binary 'a', 'a,b'), FIND_IN_SET('a', _binary 'a,b');
SELECT FIND_IN_SET(_binary 'A', 'a,b' COLLATE utf8mb4_general_ci);
SELECT FIND_IN_SET('A' COLLATE utf8mb4_general_ci, _binary 'a,b');
SELECT FIND_IN_SET(_binary 'A', 'a,b' COLLATE utf8mb4_unicode_ci);
SELECT FIND_IN_SET('A' COLLATE utf8mb4_unicode_ci, _binary 'a,b');
SELECT FIND_IN_SET(_binary 'z', 'a,b' COLLATE utf8mb4_general_ci), FIND_IN_SET(_binary 'A', 'A,b' COLLATE utf8mb4_bin);
SELECT FIND_IN_SET('a' COLLATE utf8mb4_unicode_ci, 'A,b');

-- The same rule applies when an implicit column collation wins over a binary
-- literal: the binary value compares under the column's collation (values
-- probed on the MySQL 8.0.44 oracle).
SELECT COUNT(*) FROM uci2 WHERE s LIKE _binary 'alice%';
SELECT COUNT(*) FROM uci2 WHERE _binary 'alice' LIKE s;
SELECT COUNT(*) FROM uci2 WHERE FIND_IN_SET(_binary 'ALICE', s) > 0;
SELECT COUNT(*) FROM uci2 WHERE FIND_IN_SET(s, _binary 'alice,bob') > 0;
SELECT COUNT(*) FROM ci1 WHERE name LIKE _binary 'alice%';
SELECT COUNT(*) FROM ci1 WHERE FIND_IN_SET(_binary 'CAROL', name) > 0;

-- The selected comparison collation reaches the pattern operators even when
-- no argument carries an explicit COLLATE: pairing a utf8mb4_bin column with
-- a utf8mb4_general_ci column keeps LIKE/REGEXP/FIND_IN_SET byte-wise in
-- both operand orders, while equality keeps its legacy byte comparison.
CREATE TABLE mix1 (s VARCHAR(30) COLLATE utf8mb4_general_ci, p VARCHAR(30) COLLATE utf8mb4_bin);
INSERT INTO mix1 VALUES ('A', 'a'), ('Alice', 'Alice');
SELECT s LIKE p, p LIKE s FROM mix1 ORDER BY s;
SELECT s REGEXP p, p REGEXP s FROM mix1 ORDER BY s;
SELECT s = p, p = s FROM mix1 ORDER BY s;
CREATE TABLE mix2 (bn VARCHAR(30) COLLATE utf8mb4_bin, gl VARCHAR(40) COLLATE utf8mb4_general_ci,
                   gn VARCHAR(30) COLLATE utf8mb4_general_ci, bl VARCHAR(40) COLLATE utf8mb4_bin);
INSERT INTO mix2 VALUES ('A', 'a,b', 'A', 'a,b'), ('a', 'a,b', 'a', 'a,b');
SELECT FIND_IN_SET(bn, gl), FIND_IN_SET(gn, bl) FROM mix2 ORDER BY bn;

-- LIKE compares one pattern character against one value character by their
-- collation weights (MySQL my_wildcmp_uca): utf8mb4_general_ci maps accented
-- and dotted letters onto base letters, while utf8mb4_unicode_ci applies UCA
-- per-character weights, where ı stays distinct from I. LIKE never spans an
-- expansion across characters, so under utf8mb4_unicode_ci ß matches only
-- itself, unlike FIND_IN_SET and =, which compare whole collation keys and
-- therefore equate ß with ss.
SELECT 'á' LIKE 'a', 'Á' LIKE 'a%';
SELECT 'ı' LIKE 'I', 'ß' LIKE 's', 'ß' LIKE 'ss';
SELECT 'œ' LIKE 'o', 'æ' LIKE 'a';
SELECT 'á' LIKE 'a' COLLATE utf8mb4_unicode_ci, 'áb' LIKE 'a_' COLLATE utf8mb4_unicode_ci;
SELECT 'ı' LIKE 'I' COLLATE utf8mb4_unicode_ci, 'ß' LIKE 's' COLLATE utf8mb4_unicode_ci;
SELECT 'ß' LIKE 'ss' COLLATE utf8mb4_unicode_ci, 'ss' LIKE 'ß' COLLATE utf8mb4_unicode_ci;
SELECT 'œ' LIKE 'oe' COLLATE utf8mb4_unicode_ci, 'æ' LIKE 'ae' COLLATE utf8mb4_unicode_ci;
SELECT 'ß' LIKE '_' COLLATE utf8mb4_unicode_ci, 'ß' LIKE '__' COLLATE utf8mb4_unicode_ci;
SELECT 'xßy' LIKE '%ss%' COLLATE utf8mb4_unicode_ci, 'xßy' LIKE '%s%' COLLATE utf8mb4_unicode_ci;

-- REGEXP evaluates pattern matching with MySQL's ICU engine semantics: a
-- case-insensitive literal run compares by full Unicode case folding, so ß
-- matches ss and Straße matches STRASSE under either ci collation, while
-- accented characters without a fold mapping (á, œ) stay distinct. LIKE
-- above never spans an expansion; REGEXP folds the whole literal run.
SELECT 'á' REGEXP 'a', 'á' REGEXP 'a' COLLATE utf8mb4_unicode_ci;
SELECT 'œ' REGEXP 'oe' COLLATE utf8mb4_unicode_ci, 'æ' REGEXP 'ae' COLLATE utf8mb4_unicode_ci;
SELECT 'ß' REGEXP 'ss' COLLATE utf8mb4_unicode_ci, 'ss' REGEXP 'ß' COLLATE utf8mb4_unicode_ci;
SELECT 'Straße' REGEXP 'STRASSE' COLLATE utf8mb4_unicode_ci, 'STRASSE' REGEXP 'Straße' COLLATE utf8mb4_unicode_ci;
SELECT 'ﬀ' REGEXP 'ff' COLLATE utf8mb4_unicode_ci, 'FF' REGEXP 'ﬀ' COLLATE utf8mb4_unicode_ci;
SELECT 'ß' REGEXP 'ss', 'Straße' REGEXP 'strasse';

-- On a utf8mb4_unicode_ci column the three string operators follow their
-- own contracts for the same row: equality equates ß with ss through the
-- whole collation key, REGEXP matches through full case folding, and LIKE
-- still compares per character and therefore does not.
INSERT INTO uci2 VALUES ('Straße');
SELECT s FROM uci2 WHERE s = 'strasse' ORDER BY s;
SELECT s FROM uci2 WHERE s REGEXP 'strasse' ORDER BY s;
SELECT s FROM uci2 WHERE s LIKE 'strasse' ORDER BY s;
SELECT COUNT(*) FROM uci2 WHERE s REGEXP '^strasse$';
SELECT COUNT(*) FROM uci2 WHERE s LIKE 'strasse';

-- Quantified atoms and character classes still compare per character, so ß
-- matches neither s+ nor s{2} and is not closed into [A-Z], while the
-- class orbit itself stays case-insensitive.
SELECT 'ß' REGEXP 's+' COLLATE utf8mb4_unicode_ci, 'ß' REGEXP 's{2}' COLLATE utf8mb4_unicode_ci, 'ss' REGEXP 's{2}' COLLATE utf8mb4_unicode_ci;
SELECT 'ß' REGEXP '[A-Z]' COLLATE utf8mb4_unicode_ci, 'SS' REGEXP '[A-Z]+' COLLATE utf8mb4_unicode_ci, 'ß' REGEXP '[ß]' COLLATE utf8mb4_unicode_ci;

-- U+1E9E (capital sharp s) full-folds to ss and cross-folds with ß and ss
-- in ICU caseless regex matching (ICU 78.3 / MySQL 8.0 probed for #31175).
SELECT 'ẞ' REGEXP 'ẞ' COLLATE utf8mb4_unicode_ci, 'ẞ' REGEXP 'ß' COLLATE utf8mb4_unicode_ci, 'ẞ' REGEXP 'ss' COLLATE utf8mb4_unicode_ci;
SELECT 'ß' REGEXP 'ẞ' COLLATE utf8mb4_unicode_ci, 'ss' REGEXP 'ẞ' COLLATE utf8mb4_unicode_ci;

-- REGEXP_LIKE applies the same folding through an explicit match_type, and
-- 'c' keeps the case-sensitive byte comparison.
SELECT REGEXP_LIKE('ß', 'ss', 'i'), REGEXP_LIKE('Straße', 'STRASSE', 'i');
SELECT REGEXP_LIKE('ß', 'ss', 'c'), REGEXP_LIKE('Straße', 'STRASSE', 'c');

-- An embedded (?i) scope folds the same way, while a scoped (?-i:...) keeps
-- the sensitive comparison inside an insensitive pattern.
SELECT 'ß' REGEXP '(?i)ss', 'ß' REGEXP '(?i:ss)';
SELECT 'ß' REGEXP '(?-i:ss)' COLLATE utf8mb4_unicode_ci, 'ASS' REGEXP 'a(?-i:ss)' COLLATE utf8mb4_unicode_ci, 'Ass' REGEXP 'a(?-i:ss)' COLLATE utf8mb4_unicode_ci;

-- A quantified fold-table character emits every source of its fold image,
-- so the Greek pair U+1F80/U+1F88 (both full-fold to ἀι) matches in either
-- direction exactly as ICU does; an unquantified run folds as a whole.
SELECT 'ᾀ' REGEXP 'ᾈ+' COLLATE utf8mb4_unicode_ci, 'ᾈ' REGEXP 'ᾀ+' COLLATE utf8mb4_unicode_ci;
SELECT 'ᾀ' REGEXP 'ᾈ' COLLATE utf8mb4_unicode_ci, 'ᾈ' REGEXP 'ᾀ' COLLATE utf8mb4_unicode_ci;
-- ICU additionally lets one quantified pattern character span its
-- two-character fold image in the subject (ἀι matches ᾈ+ there); MO keeps
-- the per-character boundary and pins the divergence.
SELECT 'ἀι' REGEXP 'ᾈ+' COLLATE utf8mb4_unicode_ci, 'ἀι' REGEXP 'ᾀ+' COLLATE utf8mb4_unicode_ci;

-- Escaped literal runs (\Q..\E, \x{..}, \xNN) decode into the folded run
-- instead of terminating it, so they full-fold like any other literal.
-- (SQL doubles the backslashes so the engine sees one.)
SELECT 'ß' REGEXP '\\Qss\\E' COLLATE utf8mb4_unicode_ci, 'Straße' REGEXP '\\QSTRASSE\\E' COLLATE utf8mb4_unicode_ci;
SELECT 'ß' REGEXP '\\x{73}\\x{73}' COLLATE utf8mb4_unicode_ci, 'ß' REGEXP '\\x73\\x73' COLLATE utf8mb4_unicode_ci;
SELECT 'ß' REGEXP '\\Qss\\E', 'Straße' REGEXP '\\x{53}TRASSE';

-- A named group reference keeps its exact name bytes through the fold, so
-- mixed-case group names still resolve on the backreference engine.
SELECT 'abab' REGEXP '(?<Ab>ab)\\k<Ab>' COLLATE utf8mb4_unicode_ci, 'aBaB' REGEXP '(?<Ab>ab)\\k<Ab>' COLLATE utf8mb4_unicode_ci;

-- A literal run needing more than 64 fold alternatives (ten s's give 274
-- segmentations of the ss/ſs/SS blocks) still folds exactly through a
-- factored emission instead of falling back to verbatim simple folding.
SELECT 'ßßßßß' REGEXP 'ssssssssss' COLLATE utf8mb4_unicode_ci, 'ssssssssss' REGEXP 'ßßßßß' COLLATE utf8mb4_unicode_ci;
SELECT 'ßßßßß' REGEXP 'sssssssss' COLLATE utf8mb4_unicode_ci, 'ßßßßß' REGEXP 'sssssssssss' COLLATE utf8mb4_unicode_ci;

-- FIND_IN_SET membership compares whole collation keys the way = does:
-- under utf8mb4_unicode_ci ß equals ss, and ı stays distinct from I.
SELECT FIND_IN_SET('á', 'a,b'), FIND_IN_SET('ı', 'I,x'), FIND_IN_SET('ß', 'ss,x');
SELECT FIND_IN_SET('á', 'a,b' COLLATE utf8mb4_unicode_ci), FIND_IN_SET('ı', 'I,x' COLLATE utf8mb4_unicode_ci), FIND_IN_SET('ß', 'ss,x' COLLATE utf8mb4_unicode_ci);

-- NULL, escaped wildcards and empty-pattern edges keep their semantics.
SELECT 'x' LIKE NULL, 'x' REGEXP NULL;
SELECT '' LIKE '', '' LIKE '%', 'a' LIKE '';
SELECT 'a_b' LIKE 'a\_b', 'ab' LIKE 'a\_b', 'axb' LIKE 'a\_b';
SELECT 'a%b' LIKE 'a\%b', 'a_b' LIKE 'a!_b' ESCAPE '!', 'AXB' LIKE 'a!_b' ESCAPE '!';

-- LIKE with two or more '%' wildcards splits into fixed-width segments
-- located by a leftmost greedy search; the folded segment semantics are
-- unchanged, including anchored ends and overlapping repeated segments.
SELECT 'abXXcdYYef' LIKE 'ab%cd%ef', 'abXXef' LIKE 'ab%cd%ef', 'XXabXXcdXXef' LIKE 'ab%cd%ef';
SELECT 'aaa' LIKE '%aa%a%', 'aa' LIKE '%a%a%', 'ba' LIKE '%a%a%';
SELECT 'xAliceyBobz' LIKE '%alice%bob%', 'xAlicey' LIKE '%alice%bob%';

-- ILIKE keeps folding in every string domain, preserving the complete
-- Unicode simple-fold orbit in both directions: the Kelvin and Angstrom
-- signs match their letters, and the sigma orbit stays merged.
SELECT 'ABC' ILIKE 'a_c';
SELECT 'K' ILIKE 'k', 'K' ILIKE 'K';
SELECT 'Å' ILIKE 'å', 'Å' ILIKE 'Å';
SELECT 'ς' ILIKE 'Σ', 'σ' ILIKE 'ς';
SELECT 'xKz' ILIKE '%k%', 'K_k' ILIKE 'k!_k' ESCAPE '!';

-- FIND_IN_SET over a column list value follows the column collation.
CREATE TABLE fis1 (k INT PRIMARY KEY, seg VARCHAR(40));
INSERT INTO fis1 VALUES (1, 'Red,Green'), (2, 'blue');
SELECT k FROM fis1 WHERE FIND_IN_SET('red', seg) > 0 ORDER BY k;
SELECT k FROM fis1 WHERE FIND_IN_SET('BLUE', seg) > 0 ORDER BY k;

-- A case-insensitive LIKE must not be rewritten into the byte-ordered
-- =/prefix_eq filters, so scan and index paths agree on folded rows.
CREATE INDEX ci1_name_idx ON ci1 (name);
EXPLAIN SELECT name FROM ci1 WHERE name LIKE 'alice%';
SELECT name FROM ci1 WHERE name LIKE 'alice%' ORDER BY name;

-- Supplementary characters have no UCA 4.0.0 weight, so the Unicode LIKE
-- matcher compares them by code-point identity exactly as MySQL's
-- my_uca_charcmp does: distinct characters never match, identical ones
-- still do, and '_' consumes exactly one character.
SELECT '😀' LIKE '😁' COLLATE utf8mb4_unicode_ci, '😀' LIKE '😀' COLLATE utf8mb4_unicode_ci;
SELECT '😀' LIKE '_' COLLATE utf8mb4_unicode_ci, '😀' LIKE '__' COLLATE utf8mb4_unicode_ci;
SELECT 'x😀y' LIKE '%😁%' COLLATE utf8mb4_unicode_ci, 'x😀y' LIKE '%😀%' COLLATE utf8mb4_unicode_ci;
SELECT '😀' LIKE '�' COLLATE utf8mb4_unicode_ci, '𐐀' LIKE '𐐨' COLLATE utf8mb4_unicode_ci;

-- utf8mb4_general_ci instead folds every supplementary character to one
-- replacement weight exactly as MySQL's my_tosort_unicode does, so
-- distinct supplementary characters compare equal under the default
-- collation.
SELECT '😀' LIKE '😁', '😀' LIKE '_', 'x😀y' LIKE '%😁%';
SELECT '😀' LIKE '__', '😀😀' LIKE '_', '😀' LIKE '�';

-- FIND_IN_SET tests list membership with the unpadded collation
-- comparison, so trailing spaces stay significant even under the
-- PAD SPACE utf8mb4_unicode_ci: the needle 'a' matches the member 'a' at
-- position 2, not the member 'a ' at position 1.
SELECT FIND_IN_SET('a', 'a ,a' COLLATE utf8mb4_unicode_ci), FIND_IN_SET('a ', 'a,b' COLLATE utf8mb4_unicode_ci);
SELECT FIND_IN_SET('a', 'a , a' COLLATE utf8mb4_unicode_ci), FIND_IN_SET('', 'a,,b' COLLATE utf8mb4_unicode_ci);
SELECT FIND_IN_SET('a', 'a ,a'), FIND_IN_SET('a ', 'a,b');

-- Whole-key membership keeps equating characters that share one collation
-- key, including unweighted supplementary characters and the U+00DF
-- expansion; only the per-character LIKE matcher compares identity.
SELECT FIND_IN_SET('😀', '😁,x' COLLATE utf8mb4_unicode_ci), FIND_IN_SET('😀', '😁,x');
SELECT FIND_IN_SET('ß', 'ss,x' COLLATE utf8mb4_unicode_ci);

-- The case-fold rewrite preserves free-spacing (?x) and comment syntax:
-- whitespace and comments are trivia that join the surrounding literal
-- run, which then full-folds exactly as ICU folds the stripped run.
SELECT 'ß' REGEXP '(?xi)s s', 'ß' REGEXP '(?xi)ss#comment';
SELECT 'ß' REGEXP '(?x)ss # trailing comment';
SELECT 'ßß' REGEXP '(?xi)ss # ignored\nss';
SELECT 'ß' REGEXP '(?xi)s\ts';
SELECT 'ßß' REGEXP '(?xi)ß {2}', 'ss' REGEXP '(?xi)ß {2}';
SELECT 'ab cd' REGEXP '(?xi)a(?-x:b c)d', 'abcd' REGEXP '(?xi)a(?-x:b c)d';
SELECT 'AB' REGEXP 'a(?#c)b', 'ß' REGEXP 's(?#ß)s', 'ßx' REGEXP 'ss(?#c ß)x';
SELECT 'a b' REGEXP 'a(?#c) b', 'ab' REGEXP 'a(?#c) b';
SELECT 'ab' REGEXP '(?x)a(?#c) b';

-- An unterminated (?# comment passes through untouched; this regexp2
-- engine accepts it as a comment running to the end of the pattern
-- (pre-existing engine behavior; ICU reports a compile error instead).
SELECT 'ab' REGEXP 'a(?#cb';

-- The regexp2 engine accepts the \g<name>, \g'name' and \g{name}
-- backreference spellings in addition to \k<name> (an engine extension:
-- ICU treats \g as a literal 'g'); the fold rewrite keeps every spelling
-- verbatim and the reference name opaque.
SELECT 'abab' REGEXP '(?<X>ab)\\g<X>', "abab" REGEXP "(?<X>ab)\\g'X'", 'abab' REGEXP '(?<X>ab)\\g{X}';
SELECT 'aBaB' REGEXP '(?<X>ab)\\g<X>', "aBaB" REGEXP "(?<X>ab)\\g'X'";
SELECT 'abab' REGEXP '(?<X>ab)\\g<x>';
SELECT 'aBaB' REGEXP '(?i)(?<X>ab)\\g<X>', REGEXP_LIKE('aBaB', '(?<X>ab)\\g<X>', 'i'), REGEXP_LIKE('aBaB', '(?<X>ab)\\g<X>', 'c');

-- Quoted whitespace and hash characters decode into the literal run in
-- default mode, so the surrounding text full-folds across them; under
-- (?x) a quoted space or hash stays a literal character instead of
-- becoming trivia.
SELECT 'ß ß' REGEXP '\\Qss ss\\E', 'ß#' REGEXP '\\Qss#\\E', 'ß# ß' REGEXP '\\Qss# ss\\E';
SELECT 'ss ss' REGEXP '\\Qss ss\\E', 'ssss' REGEXP '\\Qss ss\\E';
SELECT 'ß ß' REGEXP '(?xi)\\Qss ss\\E', 'ßß' REGEXP '(?xi)\\Qss ss\\E', 'ß#' REGEXP '(?xi)\\Qss#\\E';
SELECT 'ß\nß' REGEXP '\\Qss\nss\\E', 'ss\nss' REGEXP '\\Qss\nss\\E';
SELECT 'a b' REGEXP '(?x)\\Qa b\\E', 'a b' REGEXP '(?x)a\\x{20}b', 'a#b' REGEXP '(?x)a\\x{23}b';
SELECT 'ß' REGEXP '(?xi)\\x{73}\\x{73}';
SELECT 'ß' REGEXP '\\x{73}\\x{73}';
SELECT REGEXP_LIKE('SS SS', '\\Qss ss\\E', 'c'), REGEXP_LIKE('ss ss', '\\Qss ss\\E', 'c'), REGEXP_LIKE('ß#', '\\Qss#\\E', 'c');
SELECT REGEXP_LIKE('ß#', '\\Qss#\\E', 'i'), REGEXP_LIKE('ß', '(?x)ss', 'c'), REGEXP_LIKE('ß', '(?x)ss', 'i');
SELECT REGEXP_LIKE('ß', '(?xi)ss#comment'), REGEXP_LIKE('abab', '(?<X>ab)\\g{X}');

-- A free-spacing pattern against a real column folds through the same
-- comment-stripped run as the literal form.
SELECT COUNT(*) FROM ci1 WHERE name REGEXP '(?xi)alice # trailing';

-- Round-10 F1: a \UXXXXXXXX / \uXXXX / \cX / octal / multi-digit escape is
-- one complete engine token; its payload bytes never enter the literal
-- fold run. \U and \c route to the regexp2 engine, the plain octal form
-- runs on the RE2 path and a trailing (?#x) forces the regexp2 stereo
-- route. U+FF41 fullwidth a case-folds only with U+FF21 fullwidth A.
SELECT 'ａ' REGEXP '\\U0000FF41', 'Ａ' REGEXP '\\U0000FF41', 'a' REGEXP '\\U0000FF41';
SELECT 'ａß' REGEXP '\\U0000FF41ss', 'ＡSS' REGEXP '\\U0000FF41ss';
SELECT REGEXP_LIKE('ａss', '\\U0000FF41ss', 'c'), REGEXP_LIKE('ａß', '\\U0000FF41ss', 'c');
-- Lowercase \u stays on the RE2 route only when no regexp2 feature is
-- present; the (?#x) comment forces the regexp2 route for the \u token.
SELECT 'ÿß' REGEXP '\\u00FFss(?#x)', 'ŸSS' REGEXP '\\u00FFss(?#x)';
SELECT REGEXP_LIKE('ÿss', '\\u00FFss(?#x)', 'c'), REGEXP_LIKE('ÿß', '\\u00FFss(?#x)', 'c');
-- A \u/\U token is opaque to the fold run: its decoded character still
-- matches itself, but it does not join full folding (documented boundary
-- against ICU, which full-folds the decoded text).
SELECT 'ß' REGEXP '\\u00DF(?#x)', 'ss' REGEXP '\\u00DF(?#x)';
-- \cS is the single control character U+0013; a real column stores the
-- control byte through INSERT coercion so the ci collation applies.
CREATE TABLE r10c1 (s VARCHAR(40));
INSERT INTO r10c1 VALUES (CONCAT(CHAR(19),'ß')),(CONCAT(CHAR(19),'ss')),(CHAR(19));
SELECT COUNT(*) FROM r10c1 WHERE s REGEXP '\\cSss';
SELECT COUNT(*) FROM r10c1 WHERE REGEXP_LIKE(s, '\\cSss', 'c');
DROP TABLE r10c1;
-- Octal \012 is one newline character on both engine paths.
SELECT '\nß' REGEXP '\\012ss', '\nSS' REGEXP '\\012ss', '\nss' REGEXP '\\012ss';
SELECT REGEXP_LIKE('\nss', '\\012ss', 'c'), REGEXP_LIKE('\nß', '\\012ss', 'c');
SELECT '\nß' REGEXP '\\012ss(?#x)';
-- \11 is a backreference to group 11 (the letter k), not \1 then '1'.
SELECT 'abcdefghijkkß' REGEXP '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)\\11ss', 'abcdefghijkkss' REGEXP '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)\\11ss';
SELECT REGEXP_LIKE('abcdefghijkkss', '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)\\11ss', 'c'), REGEXP_LIKE('abcdefghijkkß', '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)\\11ss', 'c');

-- Round-10 F2: the regexp2 option grammar accepts the uppercase mates I
-- and X and cumulative '+' transitions, so the case-fold rewrite tracks
-- them exactly like the lowercase forms (engine extension: ICU rejects
-- these forms). They always route to the regexp2 engine.
SELECT REGEXP_LIKE('A', '(?-I)A', 'i'), REGEXP_LIKE('a', '(?-I)A', 'i');
SELECT REGEXP_LIKE('ß', '(?I)ss', 'c'), REGEXP_LIKE('SS', '(?I)ss', 'c');
SELECT REGEXP_LIKE('ß', '(?+i)ss', 'c'), REGEXP_LIKE('SS', '(?+i)ss', 'c');
SELECT REGEXP_LIKE('ß', '(?-i+I)ss', 'c'), REGEXP_LIKE('SS', '(?-i+I)ss', 'c');
-- Uppercase X enables free-spacing: the '#' comment is trivia, the run
-- around it folds, and no '#' reaches the emitted alternation.
SELECT 'ß' REGEXP '(?X)ss#comment', 'ss' REGEXP '(?X)ss#comment';
SELECT REGEXP_LIKE('ss', '(?X)ss#comment', 'c'), REGEXP_LIKE('ß', '(?X)ss#comment', 'c');
SELECT 'ss' REGEXP '(?X)s s', 's s' REGEXP '(?X)s s', 'ß' REGEXP '(?X)s s';
-- Scoped (?-I:...) compares case-sensitively inside and restores after.
SELECT 'AbC' REGEXP '(?i)a(?-I:b)c', 'ABC' REGEXP '(?i)a(?-I:b)c';
SELECT 'ssSS' REGEXP '(?i)(?-I:ss)SS', 'SSSS' REGEXP '(?i)(?-I:ss)SS';
-- Scoped (?-X:...) keeps its whitespace literal inside the scope.
SELECT 'ab cd' REGEXP '(?X)a(?-X:b c)d', 'abcd' REGEXP '(?X)a(?-X:b c)d';

-- Round-10 F3: an empty \Q\E segment carries no text, so it is
-- transparent to full folding: the literal pieces around it fold as one
-- run and a following quantifier binds across it (ICU parity). The plain
-- form runs on the RE2 path; (?#x) forces the regexp2 stereo route.
SELECT 'ß' REGEXP '^s\\Q\\Es$', 'ss' REGEXP '^s\\Q\\Es$', 's' REGEXP '^s\\Q\\Es$';
SELECT REGEXP_LIKE('ss', '^s\\Q\\Es$', 'c'), REGEXP_LIKE('ß', '^s\\Q\\Es$', 'c');
SELECT 'ß' REGEXP '^s\\Q\\Es$(?#x)';
SELECT 'aaa' REGEXP '^a\\Q\\E+$', 'a' REGEXP '^a\\Q\\E+$', '' REGEXP '^a\\Q\\E+$';
-- The interval binds the last atom of the joined run; the per-character
-- boundary still holds, so ßs does not match (ICU agrees).
SELECT 'sss' REGEXP '^ss\\Q\\E{2}$', 'ss' REGEXP '^ss\\Q\\E{2}$', 'ßs' REGEXP '^ss\\Q\\E{2}$';
-- An empty quoted segment alone matches the empty string anywhere.
SELECT 'x' REGEXP '\\Q\\E';
CREATE TABLE r10q (s VARCHAR(40));
INSERT INTO r10q VALUES ('ß'),('ss'),('s');
SELECT COUNT(*) FROM r10q WHERE s REGEXP '^s\\Q\\Es$';
SELECT COUNT(*) FROM r10q WHERE REGEXP_LIKE(s, '^s\\Q\\Es$', 'c');
DROP TABLE r10q;

-- Round-11 F1: a nonzero digit starts a decimal escape whose complete
-- digit run is one engine token (the regexp2 parser's scanDecimal), so
-- \18 names group 18 — not group 1 with a literal '8'. The fold run after
-- the token still folds; \1-\9 always route to the regexp2 engine.
SELECT 'abcdefghijklmnopqrrss' REGEXP '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\\18ss', 'abcdefghijklmnopqrrß' REGEXP '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\\18ss';
SELECT REGEXP_LIKE('abcdefghijklmnopqrrß', '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\\18ss', 'c'), REGEXP_LIKE('abcdefghijklmnopqra8ss', '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\\18ss', 'i');
SELECT 'abcdefghijklmnopqrssss' REGEXP '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)(s)\\19ss';
SELECT REGEXP_LIKE('abcdefghijklmnopqrsa9ss', '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)(s)\\19ss', 'i');
-- Octal fallback stability: with fewer groups the run falls back to
-- scanCharEscape, so \12 is one newline; these matched the same before.
SELECT 'ab\nß' REGEXP '(a)(b)\\12ss', 'ab\nss' REGEXP '(a)(b)\\12ss';
SELECT REGEXP_LIKE('ab\nß', '(a)(b)\\12ss', 'c'), REGEXP_LIKE('ab\n12ss', '(a)(b)\\12ss', 'c');
-- The \18 octal fallback reads \x01 then a literal '8'; a real column
-- stores the control byte through INSERT coercion so the ci collation
-- applies (CHAR() is binary inside a SELECT expression).
CREATE TABLE r11f (s VARCHAR(40));
INSERT INTO r11f VALUES (CONCAT('ab',CHAR(1),'8ss')),(CONCAT('ab',CHAR(1),'8ß')),('ab8ss');
SELECT COUNT(*) FROM r11f WHERE s REGEXP '(a)(b)\\18ss';
SELECT COUNT(*) FROM r11f WHERE REGEXP_LIKE(s, '(a)(b)\\18ss', 'c');
DROP TABLE r11f;

-- Round-11 F2: the short property form \pX / \PX is one escape token on
-- both engines, so its letter never joins the fold run (ICU rejects the
-- short form — an engine extension). The plain form runs on the RE2
-- path; (?#x) forces the regexp2 stereo route.
SELECT 'ass' REGEXP '\\pLss', 'aß' REGEXP '\\pLss', '1ss' REGEXP '\\pLss';
SELECT REGEXP_LIKE('ass', '\\pLss', 'c'), REGEXP_LIKE('aß', '\\pLss', 'c');
SELECT '1ss' REGEXP '\\PLss', 'aß' REGEXP '\\PLss';
SELECT 'aß' REGEXP '\\p{L}ss';
SELECT 'ass' REGEXP '\\pLss(?#x)', 'aß' REGEXP '\\pLss(?#x)';
SELECT REGEXP_LIKE('aß', '\\pLss(?#x)', 'c');

-- Round-11 F3: the regexp2 grammar nests classes for subtraction —
-- [a-z-[bc]] is a-z minus {b,c} — so the fold rewrite consumes the
-- nested set instead of closing the class at the first ']'. (?#x) forces
-- the regexp2 route; the same text without it stays on the RE2 path,
-- which has no nesting (class {a-z,-,[,b,c}, then a literal ']').
SELECT 'ass' REGEXP '[a-z-[bc]]ss(?#x)', 'zss' REGEXP '[a-z-[bc]]ss(?#x)', 'bss' REGEXP '[a-z-[bc]]ss(?#x)', 'css' REGEXP '[a-z-[bc]]ss(?#x)';
SELECT 'aß' REGEXP '[a-z-[bc]]ss(?#x)', 'ASS' REGEXP '[a-z-[bc]]ss(?#x)', 'BSS' REGEXP '[a-z-[bc]]ss(?#x)', 'bß' REGEXP '[a-z-[bc]]ss(?#x)';
SELECT REGEXP_LIKE('ass', '[a-z-[bc]]ss(?#x)', 'c'), REGEXP_LIKE('aß', '[a-z-[bc]]ss(?#x)', 'c');
-- Recursive subtraction: [a-z-[b-c-[d]]] is a-z minus {b,c}.
SELECT 'dss' REGEXP '[a-z-[b-c-[d]]]ss(?#x)', 'css' REGEXP '[a-z-[b-c-[d]]]ss(?#x)', 'dß' REGEXP '[a-z-[b-c-[d]]]ss(?#x)', 'cß' REGEXP '[a-z-[b-c-[d]]]ss(?#x)';
-- A ']' in first position is a literal member; the subtraction nests.
SELECT ']ss' REGEXP '[]-[a]]ss(?#x)', ']ß' REGEXP '[]-[a]]ss(?#x)', 'aß' REGEXP '[]-[a]]ss(?#x)';
-- RE2 route: no nesting — the second ']' is a literal character.
SELECT 'a]ss' REGEXP '[a-z-[bc]]ss', 'ass' REGEXP '[a-z-[bc]]ss', 'a]ß' REGEXP '[a-z-[bc]]ss', 'aß' REGEXP '[a-z-[bc]]ss';

-- Round-12 F1: (?>...) is a complete atomic-group opener, not a "(?flags"
-- header: the '>' never joins the fold run, so a foldable body cannot turn
-- the group into a conditional whose empty branches match every subject.
-- Atomic groups route to the regexp2 engine; the body still folds.
SELECT 'x' REGEXP '(?>ss)', 'ss' REGEXP '(?>ss)', 'ß' REGEXP '(?>ss)', 'SS' REGEXP '(?>ss)';
SELECT 'ss' REGEXP '^(?>ss)$', 'x' REGEXP '^(?>ss)$';
SELECT REGEXP_LIKE('x', '(?>ss)', 'c'), REGEXP_LIKE('SS', '(?>ss)', 'c'), REGEXP_LIKE('ss', '(?>ss)', 'c');
-- Affixed and nested atomic groups keep their scope and their folding.
SELECT 'xssy' REGEXP 'x(?>ss)y', 'xßy' REGEXP 'x(?>ss)y', 'xsy' REGEXP 'x(?>ss)y';
SELECT 'x' REGEXP '(?>(?>ss))', 'ß' REGEXP '(?>(?>ss))';
SELECT REGEXP_LIKE('ß', '(?>(?>ss))', 'c'), REGEXP_LIKE('ss', '(?>(?>ss))', 'c');
-- Atomicity is preserved: the group gives nothing back, so (?>a+)ab cannot
-- match 'aaab' where the backtracking control (a+)ab can (ICU parity).
SELECT 'aaab' REGEXP '(?>a+)ab', 'aaab' REGEXP '(a+)ab';

-- Round-12 F2: the regexp2 engine compares caselessly in ToLower space, so
-- a normalized literal must keep its complete single-character fold orbit
-- (ς->[σς], ſ->[sſ], µ->[μµ], ᲀ->[вᲀ]) or the identical-text
-- self-match is lost. (?#x) forces the regexp2 route; RE2 was already right.
SELECT 'ς' REGEXP '^ς$(?#x)', 'σ' REGEXP '^ς$(?#x)', 'Σ' REGEXP '^ς$(?#x)', 's' REGEXP '^ς$(?#x)';
SELECT 'ςς' REGEXP '^ςς$(?#x)', 'Σσ' REGEXP '^ςς$(?#x)', 'ςx' REGEXP '^ςς$(?#x)';
SELECT REGEXP_LIKE('ς', '^ς$(?#x)', 'i'), REGEXP_LIKE('σ', '^ς$(?#x)', 'c'), REGEXP_LIKE('ς', '^ς$(?#x)', 'c');
SELECT 'ſ' REGEXP '^ſ$(?#x)', 's' REGEXP '^ſ$(?#x)';
SELECT 'ſs' REGEXP '^ss$(?#x)', 'sſ' REGEXP '^ss$(?#x)', 'ſſ' REGEXP '^ss$(?#x)', 'SS' REGEXP '^ss$(?#x)';
SELECT 'ß' REGEXP '^ss$(?#x)', 'sx' REGEXP '^ss$(?#x)';
SELECT 'µ' REGEXP '^µ$(?#x)', 'μ' REGEXP '^µ$(?#x)', 'Μ' REGEXP '^µ$(?#x)', 'm' REGEXP '^µ$(?#x)';
SELECT 'ᲀ' REGEXP '^ᲀ$(?#x)', 'в' REGEXP '^ᲀ$(?#x)', 'В' REGEXP '^ᲀ$(?#x)', 'б' REGEXP '^ᲀ$(?#x)';
-- Multi-character fold image: the image runes keep their own orbits, so the
-- ypogegrammeni spellings join the fold; the source runes stay verbatim.
SELECT 'ἀι' REGEXP '^ᾀ$(?#x)', 'ᾀ' REGEXP '^ᾀ$(?#x)', 'ἀι' REGEXP '^ᾀ$(?#x)', 'ᾀ' REGEXP '^ᾀ$(?#x)', 'ᾈ' REGEXP '^ᾀ$(?#x)', 'ἀx' REGEXP '^ᾀ$(?#x)';
-- RE2 route (no comment): full-orbit matching is unchanged on this repair.
SELECT 'ς' REGEXP '^ς$', 'Σ' REGEXP '^ς$', 's' REGEXP '^ς$';
SELECT 'ſ' REGEXP '^ſ$', 'µ' REGEXP '^µ$', 'ᲀ' REGEXP '^ᲀ$';

-- Round-14: the case-fold expansion composes with the regexp2 admission
-- limits (design r5, invariants 5 and 7). The four identical 80-s
-- alternatives deduplicate to two distinct branches — the trailing (?#x)
-- comment joins the last one and forces the regexp2 route — but one 80-s
-- fold run alone compiles past the engine's 4,096-track compiled
-- backtracking-state bound, so the faithful expansion raises the bounded
-- resource-limit error instead of trading down to the engine's
-- per-character caseless comparison.
-- @regex("regexp case-fold resource limit exceeded: .*backtracking state exceeds", true)
SELECT REGEXP_LIKE(REPEAT('s', 80), CONCAT(REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80), '(?#x)'), 'i');
-- The case-sensitive control never rewrites and matches the identical text.
SELECT REGEXP_LIKE(REPEAT('s', 80), CONCAT(REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80), '(?#x)'), 'c');
-- @regex("regexp case-fold resource limit exceeded: .*backtracking state exceeds", true)
SELECT REPEAT('s', 80) REGEXP CONCAT(REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80), '(?#x)');
-- Below the compiled-state bound the deduplicated form folds fully on the
-- regexp2 route: four identical 12-s alternatives collapse to the
-- comment-bearing pair, and six ß equal twelve s under ICU full case
-- folding, exactly as the MySQL oracle.
SELECT REGEXP_LIKE(REPEAT('ß', 6), CONCAT(REPEAT('s', 12), '|', REPEAT('s', 12), '|', REPEAT('s', 12), '|', REPEAT('s', 12), '(?#x)'), 'i');
-- On the RE2 route (no comment) the same 328-byte-class shape deduplicates
-- to a single branch whose expansion is admitted — RE2 has no compiled
-- backtracking-state bound — so forty ß match eighty s under 'i'; the 'c'
-- control matches its identical branch.
SELECT REGEXP_LIKE(REPEAT('ß', 40), CONCAT(REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80)), 'i');
SELECT REGEXP_LIKE(REPEAT('s', 80), CONCAT(REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80), '|', REPEAT('s', 80)), 'c');
-- Public intervening-option proof (design r4/r5, invariant 5): the
-- unscoped (?i) setter in the middle branch changes the ambient option
-- state of the last branch, so the two byte-identical ss branches are not
-- duplicates; both are kept, and SS and ß match through the
-- case-insensitive second ss even under match_type 'c'. The (?=)-prefixed
-- form forces the regexp2 route with the same verdicts.
SELECT REGEXP_LIKE('SS', '^(?:ss|(?i)a|ss)$', 'c'), REGEXP_LIKE('ß', '^(?:ss|(?i)a|ss)$', 'c');
SELECT REGEXP_LIKE('SS', '(?=)^(?:ss|(?i)a|ss)$', 'c'), REGEXP_LIKE('ß', '(?=)^(?:ss|(?i)a|ss)$', 'c');
-- The scoped (?i:...) control restores the option state at group end, so
-- both ss occurrences share one effective state and the duplicate is
-- removed: ss still matches, SS and ß do not.
SELECT REGEXP_LIKE('ss', '^(?:ss|(?i:a)|ss)$', 'c'), REGEXP_LIKE('SS', '^(?:ss|(?i:a)|ss)$', 'c'), REGEXP_LIKE('ß', '^(?:ss|(?i:a)|ss)$', 'c');

-- Round-15: the case-fold expansion composes with the RE2 admission bound
-- as well. 999 nested captures around the foldable run 'ss' parse at
-- exactly the Go regexp parse-tree height bound unexpanded, and the
-- expansion's extra levels push the pattern over it. The source pattern is
-- admitted, so the retry reports the bounded resource-limit error instead
-- of trading down to the per-character caseless orbit (design error
-- contract); the 'c' control never rewrites and keeps its verdict.
-- @regex("regexp case-fold resource limit exceeded: .*expression nests too deeply", true)
SELECT REGEXP_LIKE('ss', CONCAT(REPEAT('(', 999), 'ss', REPEAT(')', 999)), 'i');
-- @regex("regexp case-fold resource limit exceeded: .*expression nests too deeply", true)
SELECT REGEXP_LIKE('SS', CONCAT(REPEAT('(', 999), 'ss', REPEAT(')', 999)), 'i');
-- @regex("regexp case-fold resource limit exceeded: .*expression nests too deeply", true)
SELECT REGEXP_LIKE('ß', CONCAT(REPEAT('(', 999), 'ss', REPEAT(')', 999)), 'i');
SELECT REGEXP_LIKE('ss', CONCAT(REPEAT('(', 999), 'ss', REPEAT(')', 999)), 'c');
-- The accept side of the nesting boundary keeps the full fold: 997 nested
-- captures leave room for the expansion, so the one-character subject
-- matches through the ß alternative.
SELECT REGEXP_LIKE('ß', CONCAT(REPEAT('(', 997), 'ss', REPEAT(')', 997)), 'i');
-- One more nesting level makes the pattern itself too deep: the unexpanded
-- admission rejects it, so it stays rejected with the base error.
SELECT REGEXP_LIKE('ss', CONCAT(REPEAT('(', 1000), 'ss', REPEAT(')', 1000)), 'i');

-- Round-16 F1: a malformed quantifier keeps the source pattern's verdict.
-- The fold rewrite escapes an unmatched '{' when it rewrites the pattern,
-- so validating the rewritten rule admitted 'ss{' as a literal and matched;
-- the unexpanded admission rejects it, and the repair reports the source
-- verdict the base engine gives. (?#x) forces the regexp2 route.
SELECT REGEXP_LIKE('ss{', 'ss{(?#x)', 'i');
SELECT REGEXP_LIKE('ss{', 'ss{(?#x)', 'c');
SELECT REGEXP_LIKE('ss{2', 'ss{2(?#x)', 'i');
-- The escaped form is a valid literal pattern and still matches.
SELECT REGEXP_LIKE('ss{', 'ss\\{(?#x)', 'i'), REGEXP_LIKE('ss{', 'ss\\{(?#x)', 'c');

-- Round-16 F2: the regexp2 engine's caseless class closure and
-- quantified-atom comparison compare in ToLower space, which misses the
-- full-fold sources (ToLower(ς)=ς, not σ), where ICU closes every
-- construct over the complete per-character orbit. The rewrite completes
-- the orbit on these paths: an explicit class gains the orbit members the
-- ToLower closure cannot reach, and a quantified atom with a single-rune
-- fold image emits the orbit class. (?#x) forces the regexp2 route; every
-- expected value was probed against ICU 78.3.
SELECT 'σ' REGEXP '^[ς]$(?#x)', 'ς' REGEXP '^[ς]$(?#x)', 'Σ' REGEXP '^[ς]$(?#x)', 's' REGEXP '^[ς]$(?#x)';
SELECT 'σ' REGEXP '^ς+$(?#x)', 'σς' REGEXP '^ς+$(?#x)', 'σσ' REGEXP '^ς{2}$(?#x)', 's' REGEXP '^ς+$(?#x)';
SELECT 'ſ' REGEXP '^s+$(?#x)', 'ſ' REGEXP '^[s]$(?#x)';
SELECT 'ſ' REGEXP '^[a-z]$(?#x)', 'ſ' REGEXP '^[A-Z]$(?#x)';
-- Negated classes close the same way, so the orbit mate stays excluded.
SELECT 'σ' REGEXP '^[^ς]$(?#x)', 'x' REGEXP '^[^ς]$(?#x)';
-- Orbit mates the engine's closure already covers need no addition.
SELECT 'K' REGEXP '^[k]$(?#x)', 'ẞ' REGEXP '^[ß]$(?#x)';
-- A class containing a subtraction set keeps the engine's own per-set
-- ToLower fiber entirely: ς is a raw member of [α-ω] and not excluded by
-- [σ], and ſ lowers to itself, outside [a-z] (ICU's per-set case closure
-- diverges on both corners — documented in the issue contract).
SELECT 'ς' REGEXP '^[α-ω-[σ]]$(?#x)', 'Σ' REGEXP '^[α-ω-[σ]]$(?#x)', 'β' REGEXP '^[α-ω-[σ]]$(?#x)';
SELECT 'ſ' REGEXP '^[a-z-[bc]]$(?#x)', 'b' REGEXP '^[a-z-[bc]]$(?#x)';
-- Case-sensitive controls: the completion never applies.
SELECT REGEXP_LIKE('σ', '^[ς]$(?#x)', 'c'), REGEXP_LIKE('ς', '^[ς]$(?#x)', 'c'), REGEXP_LIKE('σ', '^ς+$(?#x)', 'c'), REGEXP_LIKE('ſ', '^[a-z]$(?#x)', 'c');
-- Table level: the completion reaches real columns under an explicit
-- unicode_ci collation and under the session default general_ci alike.
CREATE TABLE orb1 (s VARCHAR(20) COLLATE utf8mb4_unicode_ci);
INSERT INTO orb1 VALUES ('σ'),('ς'),('Σ'),('s'),('x');
SELECT COUNT(*) FROM orb1 WHERE s REGEXP '^[ς]$(?#x)';
SELECT COUNT(*) FROM orb1 WHERE s REGEXP '^ς+$(?#x)';
CREATE TABLE orb2 (s VARCHAR(20));
INSERT INTO orb2 VALUES ('σ'),('ſ'),('s');
SELECT COUNT(*) FROM orb2 WHERE s REGEXP '^[ς]$(?#x)';
SELECT COUNT(*) FROM orb2 WHERE s REGEXP '^[a-z]$(?#x)';
DROP TABLE orb1;
DROP TABLE orb2;

-- Round-17 F1: an unterminated class (an escaped final bracket, a
-- truncated final escape, or no delimiter at all) keeps the source
-- pattern's verdict. The fold rewrite used to augment the truncated body
-- and read past its end; it now passes the bytes through, so both engines
-- report the same unterminated-class error as the base revision, under
-- the case-insensitive route exactly as under the case-sensitive one.
-- (?=) forces the regexp2 route.
SELECT REGEXP_LIKE('s', '(?=)[s\\]', 'i');
SELECT REGEXP_LIKE('s', '(?=)[s\\]', 'c');
SELECT REGEXP_LIKE('s', '(?=)[^s\\]', 'i');
SELECT REGEXP_LIKE('s', '(?=)[a-z\\]', 'i');
SELECT REGEXP_LIKE('s', '(?=)[\\p{L}\\]', 'i');
SELECT REGEXP_LIKE('s', '(?=)[a-z-[bc\\]', 'i');
SELECT REGEXP_LIKE('s', '(?=)[s\\', 'i');
-- A failed statement does not affect subsequent statements on the same
-- connection.
SELECT 'abc' REGEXP 'ABC', 'a' LIKE 'A';
-- A genuine closer after an escaped bracket still gains the orbit.
SELECT REGEXP_LIKE('s', '(?=)[s\\\\]', 'i'), REGEXP_LIKE('ſ', '(?=)[s\\\\]', 'i'), REGEXP_LIKE('\\', '(?=)[s\\\\]', 'i');

-- Round-17 F2: \uXXXX and \UXXXXXXXX escapes inside character classes
-- decode into the set and join the per-character fold orbit exactly like
-- literal and \x-decoded members — the case equivalence of a class no
-- longer depends on the escape spelling. (?#x) forces the regexp2 route
-- for the \u spellings; \U already selects it. Every expected value was
-- probed against ICU 78.3.
SELECT 'σ' REGEXP '^[\\u03C2]$(?#x)', 'Σ' REGEXP '^[\\u03C2]$(?#x)', 'ς' REGEXP '^[\\u03C2]$(?#x)', 'x' REGEXP '^[\\u03C2]$(?#x)';
SELECT 'σ' REGEXP '^[^\\u03C2]$(?#x)', 'x' REGEXP '^[^\\u03C2]$(?#x)';
SELECT 'ſ' REGEXP '^[\\u0073]$(?#x)', 's' REGEXP '^[\\u017F]$(?#x)', 'S' REGEXP '^[\\u017F]$(?#x)';
SELECT 'ſ' REGEXP '^[\\u0041-\\u005A]$(?#x)', 'ſ' REGEXP '^[\\u0061-\\u007A]$(?#x)', 'ſ' REGEXP '^[A-\\u005A]$(?#x)';
SELECT 'σ' REGEXP '^[\\U000003C2]$', 'ς' REGEXP '^[\\U000003C2]$', 'x' REGEXP '^[\\U000003C2]$';
-- Orbit mates the engine's closure already covers match in both
-- spellings, and a malformed escape keeps the engine's error verdict.
SELECT 'ẞ' REGEXP '^[\\u00DF]$(?#x)', 'ß' REGEXP '^[\\u1E9E]$(?#x)';
SELECT REGEXP_LIKE('x', '^[\\u12]$(?#x)', 'i');
SELECT REGEXP_LIKE('x', '^[\\u12]$(?#x)', 'c');
-- A \U without eight hex digits is not translated: the class reads 'U'
-- and the digits as literal members, so '1' matches and 'σ' does not.
SELECT '1' REGEXP '^[\\U0041]$(?#x)', 'σ' REGEXP '^[\\U0041]$(?#x)';
-- Explicit collations: unicode_ci folds, bin stays byte-wise.
SELECT 'σ' REGEXP '^[\\u03C2]$(?#x)' COLLATE utf8mb4_unicode_ci;
SELECT 'σ' REGEXP '^[\\u03C2]$(?#x)' COLLATE utf8mb4_bin;
-- Case-sensitive controls: the decode never applies.
SELECT REGEXP_LIKE('σ', '^[\\u03C2]$(?#x)', 'c'), REGEXP_LIKE('ς', '^[\\u03C2]$(?#x)', 'c');
-- Table level: real columns under an explicit unicode_ci collation and
-- under the session default general_ci alike.
CREATE TABLE orb3 (s VARCHAR(20) COLLATE utf8mb4_unicode_ci);
INSERT INTO orb3 VALUES ('σ'),('ς'),('Σ'),('x');
SELECT COUNT(*) FROM orb3 WHERE s REGEXP '^[\\u03C2]$(?#x)';
SELECT COUNT(*) FROM orb3 WHERE s REGEXP '^[^\\u03C2]$(?#x)';
CREATE TABLE orb4 (s VARCHAR(20));
INSERT INTO orb4 VALUES ('σ'),('ſ'),('s'),('x');
SELECT COUNT(*) FROM orb4 WHERE s REGEXP '^[\\u03C2]$(?#x)';
SELECT COUNT(*) FROM orb4 WHERE s REGEXP '^[\\u0073]$(?#x)';
DROP TABLE orb3;
DROP TABLE orb4;

-- Round-18 F1: a \uXXXX or \UXXXXXXXX escape the regexp2 engine decodes
-- into one literal atom receives the same per-character fold orbit as
-- the literal spelling — the engine's caseless ToLower fiber misses
-- orbit members (ſ for \u0073, σ for \u03C2). (?#x) forces the regexp2
-- route for the \u spellings; \U already selects it. Every expected
-- value was probed against ICU 78.3.
SELECT 'σ' REGEXP '^\\u03C2$(?#x)', 'Σ' REGEXP '^\\u03C2$(?#x)', 'ς' REGEXP '^\\u03C2$(?#x)', 'x' REGEXP '^\\u03C2$(?#x)';
SELECT 'ſ' REGEXP '^\\u0073$(?#x)', 's' REGEXP '^\\u017F$(?#x)', 'S' REGEXP '^\\u017F$(?#x)', 'ſ' REGEXP '^\\u0053$(?#x)';
SELECT 'σ' REGEXP '^\\U000003C2$', 'ς' REGEXP '^\\U000003C2$', 'x' REGEXP '^\\U000003C2$';
-- A following quantifier binds the completed atom exactly as it binds
-- the literal spelling.
SELECT 'σς' REGEXP '^\\u03C2+$(?#x)', 'Σ' REGEXP '^\\u03C2+$(?#x)', 'x' REGEXP '^\\u03C2+$(?#x)';
SELECT 'Σς' REGEXP '^\\u03C2{2}$(?#x)', 'σ' REGEXP '^\\u03C2{2}$(?#x)', 'σς' REGEXP '^\\u03C2$(?#x)';
-- The completed atom stays a single token: an escaped sharp s keeps its
-- per-character orbit and does not full-fold into ss (ICU decodes the
-- escape into the pattern text and full-folds it — the documented
-- divergence).
SELECT 'ß' REGEXP '^\\u00DF$(?#x)', 'ẞ' REGEXP '^\\u00DF$(?#x)', 'ss' REGEXP '^\\u00DF$(?#x)';
SELECT 'ß' REGEXP '^\\u1E9E$(?#x)', 'ss' REGEXP '^\\u00DF+$(?#x)', 'ßẞ' REGEXP '^\\u00DF+$(?#x)';
-- Explicit collations: unicode_ci folds, bin stays byte-wise.
SELECT 'ſ' REGEXP '^\\u0073$(?#x)' COLLATE utf8mb4_unicode_ci;
SELECT 'ſ' REGEXP '^\\u0073$(?#x)' COLLATE utf8mb4_bin;
-- Case-sensitive controls: the decode never applies.
SELECT REGEXP_LIKE('σ', '^\\u03C2$(?#x)', 'c'), REGEXP_LIKE('ς', '^\\u03C2$(?#x)', 'c');
SELECT REGEXP_LIKE('ſ', '^\\u0073$(?#x)', 'c'), REGEXP_LIKE('s', '^\\u0073$(?#x)', 'c');
-- Malformed escapes keep the engine's error verdict under both modes.
SELECT REGEXP_LIKE('u', '^\\u12$(?#x)', 'i');
SELECT REGEXP_LIKE('u', '^\\u12$(?#x)', 'c');
-- The literal run around the decoded token still full-folds.
SELECT 'ａß' REGEXP '^\\U0000FF41ss$(?#x)', 'Ａß' REGEXP '^\\U0000FF41ss$(?#x)';
-- Table level: real columns under an explicit unicode_ci collation and
-- under the session default general_ci alike.
CREATE TABLE orb5 (s VARCHAR(20) COLLATE utf8mb4_unicode_ci);
INSERT INTO orb5 VALUES ('σ'),('ſ'),('x');
SELECT COUNT(*) FROM orb5 WHERE s REGEXP '^\\u03C2$(?#x)';
SELECT COUNT(*) FROM orb5 WHERE s REGEXP '^\\u0073$(?#x)';
CREATE TABLE orb6 (s VARCHAR(20));
INSERT INTO orb6 VALUES ('σ'),('ſ'),('x');
SELECT COUNT(*) FROM orb6 WHERE s REGEXP '^\\u03C2$(?#x)';
SELECT COUNT(*) FROM orb6 WHERE s REGEXP '^\\u0073$(?#x)';
DROP TABLE orb5;
DROP TABLE orb6;

-- Round-18 F2: FIND_IN_SET prepares the needle's comparison form once
-- per row instead of per unsuccessful member; the membership semantics
-- are unchanged, including a long needle against a long list under both
-- ci collations.
SELECT FIND_IN_SET(REPEAT('z', 60000), REPEAT('a,', 30000));
SELECT FIND_IN_SET(REPEAT('Z', 600), CONCAT(REPEAT('a,', 300), REPEAT('z', 600)) COLLATE utf8mb4_unicode_ci);
SELECT FIND_IN_SET(REPEAT('Z', 600), CONCAT(REPEAT('a,', 300), REPEAT('z', 600)));

-- Round-21 review: a CAST(x AS BINARY) expression carries the same binary
-- string domain as a _binary literal, and the MySQL 8.0.44 oracle applies
-- the same coercibility rule. An explicit ci COLLATE on either operand wins
-- strictly, so the converted value compares under that collation; the
-- conversion cast must deliver the value intact and the folded result must
-- keep the selected comparison domain. An equal-coercibility tie and an
-- explicit utf8mb4_bin stay byte-wise.
SELECT CAST('abc' AS BINARY) LIKE ('ABC%' COLLATE utf8mb4_general_ci);
SELECT ('abc' COLLATE utf8mb4_general_ci) LIKE CAST('ABC%' AS BINARY);
SELECT CAST('abc' AS BINARY) LIKE ('ABC%' COLLATE utf8mb4_unicode_ci);
SELECT ('abc' COLLATE utf8mb4_unicode_ci) LIKE CAST('ABC%' AS BINARY);
SELECT CAST('abc' AS BINARY) LIKE 'ABC%', CAST('abc' AS BINARY) LIKE ('ABC%' COLLATE utf8mb4_bin);
SELECT CAST('abc' AS BINARY) LIKE ('abc%' COLLATE utf8mb4_general_ci), CAST('xyz' AS BINARY) LIKE ('ABC%' COLLATE utf8mb4_general_ci);
SELECT FIND_IN_SET(CAST('A' AS BINARY), 'a,b' COLLATE utf8mb4_general_ci);
SELECT FIND_IN_SET('A' COLLATE utf8mb4_general_ci, CAST('a,b' AS BINARY));
SELECT FIND_IN_SET(CAST('A' AS BINARY), 'a,b' COLLATE utf8mb4_unicode_ci);
SELECT FIND_IN_SET('A' COLLATE utf8mb4_unicode_ci, CAST('a,b' AS BINARY));
SELECT FIND_IN_SET(CAST('A' AS BINARY), 'a,b'), FIND_IN_SET(CAST('a' AS BINARY), 'a,b' COLLATE utf8mb4_general_ci);
SELECT FIND_IN_SET(CAST('z' AS BINARY), 'a,b' COLLATE utf8mb4_general_ci), FIND_IN_SET(CAST('a' AS BINARY), 'a,b' COLLATE utf8mb4_bin);
-- Equality and IN over the CAST shape behave exactly as their pure literal
-- counterparts: utf8mb4_unicode_ci folds, while explicit utf8mb4_general_ci
-- on literals keeps the pre-existing equality parity gap that is scoped to
-- a follow-up issue ('ALICE' = 'alice' COLLATE utf8mb4_general_ci is 0 on
-- the base commit as well).
SELECT CAST('ALICE' AS BINARY) = ('Alice' COLLATE utf8mb4_general_ci);
SELECT ('Alice' COLLATE utf8mb4_general_ci) = CAST('ALICE' AS BINARY);
SELECT CAST('ALICE' AS BINARY) = ('Alice' COLLATE utf8mb4_unicode_ci);
SELECT ('Alice' COLLATE utf8mb4_unicode_ci) = CAST('ALICE' AS BINARY);
SELECT CAST('ALICE' AS BINARY) = 'Alice', CAST('ALICE' AS BINARY) = ('Alice' COLLATE utf8mb4_bin);
SELECT CAST('ALICE' AS BINARY) IN ('Alice' COLLATE utf8mb4_general_ci), CAST('ALICE' AS BINARY) IN ('Alice');

-- A CAST(x AS BINARY) operand mixed with a text collation keeps MySQL's
-- error 3995 rejection for REGEXP and REGEXP_LIKE in either operand order,
-- a COLLATE clause on the CAST itself keeps the error 1253 rejection, and
-- the CONVERT(x USING BINARY) shape already folds through the runtime path.
SELECT CAST('abc' AS BINARY) REGEXP ('ABC' COLLATE utf8mb4_general_ci);
SELECT ('ABC' COLLATE utf8mb4_general_ci) REGEXP CAST('abc' AS BINARY);
SELECT CAST('abc' AS BINARY) REGEXP 'ABC';
SELECT 'abc' REGEXP CAST('ABC' AS BINARY);
SELECT REGEXP_LIKE(CAST('abc' AS BINARY), 'ABC' COLLATE utf8mb4_general_ci);
SELECT REGEXP_LIKE('ABC' COLLATE utf8mb4_general_ci, CAST('abc' AS BINARY));
SELECT (CAST('abc' AS BINARY) COLLATE utf8mb4_general_ci) LIKE 'ABC%';
SELECT CONVERT('abc' USING BINARY) LIKE ('ABC%' COLLATE utf8mb4_general_ci);

-- Round-22 review: an implicit case-insensitive column and CAST(x AS
-- BINARY) both carry implicit coercibility, and MySQL lets the binary
-- operand absorb that tie. LIKE/ILIKE/FIND_IN_SET compare byte-wise (the
-- routing they had before joining collation resolution, and which the
-- version-zero utf8mb4_general_ci column never lost), the regexp functions
-- keep their error 3995 rejection, and equality keeps its pre-existing
-- 1267 verdict. A dominated _binary literal still folds under the column's
-- collation, and a text/text conflict stays illegal wherever MySQL's
-- pairwise aggregation reaches it before the binary operand.
CREATE TABLE bintie1 (s VARCHAR(20) COLLATE utf8mb4_unicode_ci, g VARCHAR(20));
INSERT INTO bintie1 VALUES ('Alice','Alice'), ('BOB','BOB');
SELECT COUNT(*) FROM bintie1 WHERE s LIKE CAST('alice%' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE CAST('alice%' AS BINARY) LIKE s;
SELECT COUNT(*) FROM bintie1 WHERE s LIKE CAST('Alice%' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE s NOT LIKE CAST('Alice%' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE s LIKE _binary 'alice%';
SELECT COUNT(*) FROM bintie1 WHERE FIND_IN_SET(s, CAST('alice,bob' AS BINARY)) > 0;
SELECT COUNT(*) FROM bintie1 WHERE FIND_IN_SET(s, CAST('Alice,BOB' AS BINARY)) > 0;
SELECT COUNT(*) FROM bintie1 WHERE FIND_IN_SET(CAST('alice' AS BINARY), s) > 0;
SELECT COUNT(*) FROM bintie1 WHERE FIND_IN_SET(CAST('Alice' AS BINARY), s) > 0;
SELECT COUNT(*) FROM bintie1 WHERE s ILIKE CAST('alice%' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE CAST('bob' AS BINARY) ILIKE s;
SELECT COUNT(*) FROM bintie1 WHERE g LIKE CAST('alice%' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE g LIKE CAST('Alice%' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE FIND_IN_SET(g, CAST('alice,bob' AS BINARY)) > 0;
SELECT COUNT(*) FROM bintie1 WHERE s LIKE CAST('a%' AS BINARY) ESCAPE CAST('!' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE s LIKE CAST('A%' AS BINARY) ESCAPE CAST('!' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE s REGEXP CAST('^alice' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE CAST('Alice' AS BINARY) REGEXP s;
SELECT COUNT(*) FROM bintie1 WHERE REGEXP_LIKE(s, CAST('^alice' AS BINARY));
SELECT COUNT(*) FROM bintie1 WHERE s = CAST('ALICE' AS BINARY);
SELECT COUNT(*) FROM bintie1 WHERE s LIKE g;
SELECT COUNT(*) FROM bintie1 WHERE s LIKE g ESCAPE CAST('!' AS BINARY);
-- A non-constant binary operand keeps its string domain through
-- FIND_IN_SET's and LIKE's overload matching, so a VARBINARY column
-- compares byte-wise exactly like the folded constant forms.
CREATE TABLE bintie2 (s VARCHAR(20) COLLATE utf8mb4_unicode_ci, b VARBINARY(20), p VARBINARY(20));
INSERT INTO bintie2 VALUES ('Alice','Alice,bob','alice%'), ('BOB','alice,bob','BO%');
SELECT FIND_IN_SET(s, b) FROM bintie2 ORDER BY s;
SELECT COUNT(*) FROM bintie2 WHERE s LIKE p;
DROP TABLE bintie2;
-- Prepared parameters stay non-constant at bind time, so they exercise the
-- same runtime domain routing: byte-wise membership and LIKE in both the
-- mismatching and the exact-case forms.
PREPARE bintie_like FROM 'SELECT COUNT(*) FROM bintie1 WHERE s LIKE CAST(? AS BINARY)';
SET @bintie_p = 'alice%';
EXECUTE bintie_like USING @bintie_p;
SET @bintie_p = 'Alice%';
EXECUTE bintie_like USING @bintie_p;
DEALLOCATE PREPARE bintie_like;
PREPARE bintie_fis FROM 'SELECT FIND_IN_SET(s, CAST(? AS BINARY)) FROM bintie1 ORDER BY s';
SET @bintie_l = 'alice,bob';
EXECUTE bintie_fis USING @bintie_l;
SET @bintie_l = 'Alice,bob';
EXECUTE bintie_fis USING @bintie_l;
DEALLOCATE PREPARE bintie_fis;
DROP TABLE bintie1;

DROP DATABASE IF EXISTS issue_31175;
