/**
 *  @file       lib.c
 *  @brief      JavaScript bindings for SimSIMD.
 *  @author     Ash Vardanian
 *  @date       October 18, 2023
 *
 *  @copyright  Copyright (c) 2023
 *  @see        NodeJS docs: https://nodejs.org/api/n-api.html
 */

#include <node_api.h>        // `napi_*` functions
#include <simsimd/simsimd.h> // `simsimd_*` functions

/// @brief  Global variable that caches the CPU capabilities, and is computed just onc, when the module is loaded.
simsimd_capability_t static_capabilities = simsimd_cap_serial_k;

napi_value dense(napi_env env, napi_callback_info info, simsimd_metric_kind_t metric_kind,
                 simsimd_datatype_t datatype) {
    size_t argc = 2;
    napi_value args[2];
    napi_status status;

    // Get callback info and ensure the argument count is correct
    status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    if (status != napi_ok || argc != 2) {
        napi_throw_error(env, NULL, "Wrong number of arguments");
        return NULL;
    }

    // Obtain the typed arrays from the arguments
    void *data_a, *data_b;
    size_t length_a, length_b;
    napi_typedarray_type type_a, type_b;
    napi_status status_a, status_b;
    status_a = napi_get_typedarray_info(env, args[0], &type_a, &length_a, &data_a, NULL, NULL);
    status_b = napi_get_typedarray_info(env, args[1], &type_b, &length_b, &data_b, NULL, NULL);
    if (status_a != napi_ok || status_b != napi_ok || type_a != type_b || length_a != length_b) {
        napi_throw_error(env, NULL, "Both arguments must be typed arrays of matching types and dimensionality");
        return NULL;
    }
    if (type_a != napi_float64_array && type_a != napi_float32_array && //
        type_a != napi_int8_array && type_a != napi_uint8_array) {
        napi_throw_error(env, NULL,
                         "Only `float64`, `float32`, `int8` and `uint8` arrays are supported in JavaScript bindings");
        return NULL;
    }

    if (datatype == simsimd_datatype_unknown_k) switch (type_a) {
        case napi_float64_array: datatype = simsimd_datatype_f64_k; break;
        case napi_float32_array: datatype = simsimd_datatype_f32_k; break;
        case napi_int8_array: datatype = simsimd_datatype_i8_k; break;
        case napi_uint8_array: datatype = simsimd_datatype_u8_k; break;
        default: break;
        }

    simsimd_metric_dense_punned_t metric = NULL;
    simsimd_capability_t capability = simsimd_cap_serial_k;
    simsimd_find_kernel_punned(metric_kind, datatype, static_capabilities, simsimd_cap_any_k,
                               (simsimd_kernel_punned_t *)&metric, &capability);
    if (metric == NULL) {
        napi_throw_error(env, NULL, "Unsupported datatype for given metric");
        return NULL;
    }

    simsimd_distance_t result;
    metric(data_a, data_b, length_a, &result);

    // Convert the result to a JavaScript number
    napi_value js_result;
    status = napi_create_double(env, result, &js_result);
    if (status != napi_ok) return NULL;

    return js_result;
}

napi_value api_ip(napi_env env, napi_callback_info info) {
    return dense(env, info, simsimd_metric_dot_k, simsimd_datatype_unknown_k);
}
napi_value api_cos(napi_env env, napi_callback_info info) {
    return dense(env, info, simsimd_metric_cosine_k, simsimd_datatype_unknown_k);
}
napi_value api_l2sq(napi_env env, napi_callback_info info) {
    return dense(env, info, simsimd_metric_sqeuclidean_k, simsimd_datatype_unknown_k);
}
napi_value api_l2(napi_env env, napi_callback_info info) {
    return dense(env, info, simsimd_metric_l2_k, simsimd_datatype_unknown_k);
}
napi_value api_kl(napi_env env, napi_callback_info info) {
    return dense(env, info, simsimd_metric_kl_k, simsimd_datatype_unknown_k);
}
napi_value api_js(napi_env env, napi_callback_info info) {
    return dense(env, info, simsimd_metric_js_k, simsimd_datatype_unknown_k);
}
napi_value api_hamming(napi_env env, napi_callback_info info) {
    return dense(env, info, simsimd_metric_hamming_k, simsimd_datatype_b8_k);
}
napi_value api_jaccard(napi_env env, napi_callback_info info) {
    return dense(env, info, simsimd_metric_jaccard_k, simsimd_datatype_b8_k);
}

napi_value Init(napi_env env, napi_value exports) {

    // Define an array of property descriptors
    napi_property_descriptor dot_descriptor = {"dot", 0, api_ip, 0, 0, 0, napi_default, 0};
    napi_property_descriptor inner_descriptor = {"inner", 0, api_ip, 0, 0, 0, napi_default, 0};
    napi_property_descriptor sqeuclidean_descriptor = {"sqeuclidean", 0, api_l2sq, 0, 0, 0, napi_default, 0};
    napi_property_descriptor euclidean_descriptor = {"euclidean", 0, api_l2, 0, 0, 0, napi_default, 0};
    napi_property_descriptor cosine_descriptor = {"cosine", 0, api_cos, 0, 0, 0, napi_default, 0};
    napi_property_descriptor hamming_descriptor = {"hamming", 0, api_hamming, 0, 0, 0, napi_default, 0};
    napi_property_descriptor jaccard_descriptor = {"jaccard", 0, api_jaccard, 0, 0, 0, napi_default, 0};
    napi_property_descriptor kl_descriptor = {"kullbackleibler", 0, api_kl, 0, 0, 0, napi_default, 0};
    napi_property_descriptor js_descriptor = {"jensenshannon", 0, api_js, 0, 0, 0, napi_default, 0};
    napi_property_descriptor properties[] = {
        dot_descriptor,     inner_descriptor,   sqeuclidean_descriptor, euclidean_descriptor, cosine_descriptor,
        hamming_descriptor, jaccard_descriptor, kl_descriptor,          js_descriptor,
    };

    // Define the properties on the `exports` object
    size_t property_count = sizeof(properties) / sizeof(properties[0]);
    napi_define_properties(env, exports, property_count, properties);

    static_capabilities = simsimd_capabilities();
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
