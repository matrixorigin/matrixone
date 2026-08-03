-- onnx_run: evaluate ONNX models from SQL.
-- Models live under $resources/onnx/ and are passed either as a datalink or as
-- raw varbinary bytes loaded via load_file.

-- ============================================================
-- 1. sum_and_difference: a plain single-tensor model.
--    input  [1,1,4] float32, output [1,1,2] float32.
--    The network returns [sum_of_inputs, max_pairwise_difference].
-- ============================================================

-- datalink form (overload 1)
select onnx_run(
    cast('file://$resources/onnx/sum_and_difference.onnx' as datalink),
    '[0.2,0.3,0.6,0.9]',
    '{"dim":[1,1,4],"dtype":"float32"}',
    '{"dim":[1,1,2],"dtype":"float32"}') as sum_diff;

-- varbinary form (overload 0): same model via load_file
select onnx_run(
    cast(load_file(cast('file://$resources/onnx/sum_and_difference.onnx' as datalink)) as varbinary),
    '[0.2,0.3,0.6,0.9]',
    '{"dim":[1,1,4],"dtype":"float32"}',
    '{"dim":[1,1,2],"dtype":"float32"}') as sum_diff_varbin;

-- ============================================================
-- 2. non_tensor_outputs: sklearn random forest on iris.
--    output_shape NULL -> the model's label tensor + probability
--    sequence-of-maps are returned as a json object keyed by output name.
-- ============================================================
select onnx_run(
    cast('file://$resources/onnx/sklearn_randomforest.onnx' as datalink),
    '[5.9,3.0,5.1,1.8, 6.8,2.8,4.8,1.4, 6.3,2.3,4.4,1.3, 6.5,3.0,5.5,1.8, 7.7,2.8,6.7,2.0, 5.5,2.5,4.0,1.3]',
    '{"dim":[6,4],"dtype":"float32"}',
    NULL) as forest_batch;

-- ============================================================
-- 3. Per-row inference over the checked-in iris dataset, building the
--    input tensor with json_array(). The constant model datalink lets the
--    operator cache one session and reuse it across all rows. We compare the
--    model's predicted label against the dataset's true label.
-- ============================================================
drop table if exists iris;
create table iris (id int primary key auto_increment, sl float, sw float, pl float, pw float, label int);
load data infile '$resources/onnx/iris.csv' into table iris fields terminated by ',' (sl, sw, pl, pw, label);

select
    id,
    label as true_label,
    json_unquote(json_extract(
        onnx_run(
            cast('file://$resources/onnx/sklearn_randomforest.onnx' as datalink),
            cast(json_array(sl, sw, pl, pw) as varchar),
            '{"dim":[1,4],"dtype":"float32"}',
            NULL),
        '$.output_label[0]')) as predicted_label
from iris
where id in (1, 40, 55, 75, 100, 120, 150)
order by id;

-- ============================================================
-- 4. mnist (bonus): float32 conv net, 1x1x28x28 -> 1x10 logits.
--    An all-zero image gives a deterministic logit vector.
-- ============================================================
select onnx_run(
    cast('file://$resources/onnx/mnist.onnx' as datalink),
    '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]',
    '{"dim":[1,1,28,28],"dtype":"float32"}',
    '{"dim":[1,10],"dtype":"float32"}') as mnist_logits;

-- ============================================================
-- 5. mnist_float16 (bonus): exercises the float16 in/out path.
-- ============================================================
select onnx_run(
    cast('file://$resources/onnx/mnist_float16.onnx' as datalink),
    '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]',
    '{"dim":[1,1,28,28],"dtype":"float16"}',
    '{"dim":[1,10],"dtype":"float16"}') as mnist16_logits;

-- ============================================================
-- 6. Error paths: clean SQL errors, no crash.
-- ============================================================
-- unsupported dtype
select onnx_run(
    cast('file://$resources/onnx/sum_and_difference.onnx' as datalink),
    '[0.2,0.3,0.6,0.9]',
    '{"dim":[1,1,4],"dtype":"complex64"}',
    '{"dim":[1,1,2],"dtype":"float32"}');

-- input length does not match shape
select onnx_run(
    cast('file://$resources/onnx/sum_and_difference.onnx' as datalink),
    '[0.2,0.3,0.6]',
    '{"dim":[1,1,4],"dtype":"float32"}',
    '{"dim":[1,1,2],"dtype":"float32"}');

-- non-integer input for an integer dtype is an error, not silently 0
select onnx_run(
    cast('file://$resources/onnx/sum_and_difference.onnx' as datalink),
    '[1.5,2,3,4]',
    '{"dim":[1,1,4],"dtype":"int32"}',
    '{"dim":[1,1,2],"dtype":"float32"}');

-- out-of-range input for the declared integer width is an error, not a wrap
select onnx_run(
    cast('file://$resources/onnx/sum_and_difference.onnx' as datalink),
    '[300,0,0,0]',
    '{"dim":[1,1,4],"dtype":"int8"}',
    '{"dim":[1,1,2],"dtype":"float32"}');

-- NULL model / input -> NULL result
select onnx_run(
    cast(NULL as datalink),
    '[0.2,0.3,0.6,0.9]',
    '{"dim":[1,1,4],"dtype":"float32"}',
    '{"dim":[1,1,2],"dtype":"float32"}') as null_model;

drop table if exists iris;
