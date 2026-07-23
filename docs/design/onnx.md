# Add a function that can evaluate/run an ONNX model.

Add a builtin function `onnx_run`,  
```
    select onnx_run(model, input, input_shape, output_shape) from T;

    Parameters: 
        .model, this should be a varbinary or a datalink to a file in a stage
        .input, a json object, which can be turned into a input tensor according to the input tensor shape
        .input_shape, a json object that specifies the shape of the input tensor. 
        .output_shape, a json object that specifies the shape of the output tensor.  NULL if the output is not a tensor.

    Returns: A json object of onnx output.   If the output is a tensor, the json object should be an json array of
        the specified output_shape.   Otherwise, it is just a json object that encodes whatever the onnx network output.

    Shape: A json object that specifies dimensions and types, for example, '{"dim": [1, 1, 4], "dtype": "int16"}'
        dim: dimensions
        dtype: data type as specified in onnxruntime.go, for example C.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT8 should be specifified as int8
```

## Impl

We should use `github.com/yalue/onnxruntime_go`.   Put most of ONNX runtime related code in @pkg/mlai/onnx.  Use init in this package
to load onnxruntime.so, which should be installed in the `third_party/install/lib`, and later along side with mo-serivce binary.
Make sure check the error of ONNX runtime initialization.   If failed to initialize, later ALL function call onnx should fail, but the 
database should start normally.

For each sql query calling `onnx_run`, the colexec should try to cache a onnx session, and reuse the session to evaluate model on each
input.  The session should be closed when the function expression is Released. 

## Test

Convert the examples in [onnxruntime go examples](https://github.com/yalue/onnxruntime_go_examples) to bvt tests.   
We should have tests on at least `sum_and_difference`, `non_tensor_outputs` working.   Extra bonus for `mnist`.

Take their onnx network and check in to our test resource dir.   Check in the iris dataset used in `non_tensor_outputs` as well.  
Do not check in the python scripts that generated these networks.
 





