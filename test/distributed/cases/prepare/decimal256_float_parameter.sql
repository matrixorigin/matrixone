-- @desc:Prepared text parameters are coerced symmetrically with DECIMAL256 arithmetic operands

SET @issue_28766_text = '1.2';

PREPARE issue_28766_left FROM
    'SELECT
        ? + CAST(1.0 AS DECIMAL(65, 30)) AS plus_result,
        ? - CAST(1.0 AS DECIMAL(65, 30)) AS minus_result,
        ? * CAST(1.0 AS DECIMAL(65, 30)) AS multiply_result,
        ? / CAST(1.0 AS DECIMAL(65, 30)) AS divide_result,
        ? % CAST(1.0 AS DECIMAL(65, 30)) AS mod_result,
        ? DIV CAST(1.0 AS DECIMAL(65, 30)) AS integer_div_result';
PREPARE issue_28766_right FROM
    'SELECT
        CAST(1.0 AS DECIMAL(65, 30)) + ? AS plus_result,
        CAST(1.0 AS DECIMAL(65, 30)) - ? AS minus_result,
        CAST(1.0 AS DECIMAL(65, 30)) * ? AS multiply_result,
        CAST(1.0 AS DECIMAL(65, 30)) / ? AS divide_result,
        CAST(1.0 AS DECIMAL(65, 30)) % ? AS mod_result,
        CAST(1.0 AS DECIMAL(65, 30)) DIV ? AS integer_div_result';

EXECUTE issue_28766_left USING
    @issue_28766_text, @issue_28766_text, @issue_28766_text,
    @issue_28766_text, @issue_28766_text, @issue_28766_text;
EXECUTE issue_28766_right USING
    @issue_28766_text, @issue_28766_text, @issue_28766_text,
    @issue_28766_text, @issue_28766_text, @issue_28766_text;

SET @issue_28766_text = CAST('1.2' AS DECIMAL(65, 30));
EXECUTE issue_28766_left USING
    @issue_28766_text, @issue_28766_text, @issue_28766_text,
    @issue_28766_text, @issue_28766_text, @issue_28766_text;
EXECUTE issue_28766_right USING
    @issue_28766_text, @issue_28766_text, @issue_28766_text,
    @issue_28766_text, @issue_28766_text, @issue_28766_text;

SET @issue_28766_text = '1.2';
EXECUTE issue_28766_left USING
    @issue_28766_text, @issue_28766_text, @issue_28766_text,
    @issue_28766_text, @issue_28766_text, @issue_28766_text;
EXECUTE issue_28766_right USING
    @issue_28766_text, @issue_28766_text, @issue_28766_text,
    @issue_28766_text, @issue_28766_text, @issue_28766_text;

DEALLOCATE PREPARE issue_28766_left;
DEALLOCATE PREPARE issue_28766_right;
SELECT 1 AS after_cleanup;
