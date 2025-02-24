/**
 *  @file   test.c
 *  @brief  Test focusing only on the simplest functionality.
 */

#include <assert.h> // `assert`
#include <math.h>   // `sqrtf`
#include <stdio.h>  // `printf`

#define SIMSIMD_NATIVE_F16 0
#define SIMSIMD_NATIVE_BF16 0
#include <simsimd/simsimd.h>

/**
 *  @brief  Logs CPU capabilities supported by the current build (compile-time) and runtime.
 */
void print_capabilities(void) {
    simsimd_capability_t runtime_caps = simsimd_capabilities();

    // Log supported functionality
    char const *flags[2] = {"false", "true"};
    printf("Benchmarking Similarity Measures\n");
    printf("- Compiler used native F16: %s\n", flags[SIMSIMD_NATIVE_F16]);
    printf("- Compiler used native BF16: %s\n", flags[SIMSIMD_NATIVE_BF16]);
    printf("\n");
    printf("Compile-time settings:\n");
    printf("- Arm NEON support enabled: %s\n", flags[SIMSIMD_TARGET_NEON]);
    printf("- Arm SVE support enabled: %s\n", flags[SIMSIMD_TARGET_SVE]);
    printf("- Arm SVE2 support enabled: %s\n", flags[SIMSIMD_TARGET_SVE2]);
    printf("- x86 Haswell support enabled: %s\n", flags[SIMSIMD_TARGET_HASWELL]);
    printf("- x86 Skylake support enabled: %s\n", flags[SIMSIMD_TARGET_SKYLAKE]);
    printf("- x86 Ice Lake support enabled: %s\n", flags[SIMSIMD_TARGET_ICE]);
    printf("- x86 Genoa support enabled: %s\n", flags[SIMSIMD_TARGET_GENOA]);
    printf("- x86 Sapphire Rapids support enabled: %s\n", flags[SIMSIMD_TARGET_SAPPHIRE]);
    printf("- x86 Turin support enabled: %s\n", flags[SIMSIMD_TARGET_TURIN]);
    printf("- x86 Sierra Forest support enabled: %s\n", flags[SIMSIMD_TARGET_SIERRA]);
    printf("\n");
    printf("Run-time settings:\n");
    printf("- Arm NEON support enabled: %s\n", flags[(runtime_caps & simsimd_cap_neon_k) != 0]);
    printf("- Arm NEON F16 support enabled: %s\n", flags[(runtime_caps & simsimd_cap_neon_f16_k) != 0]);
    printf("- Arm NEON BF16 support enabled: %s\n", flags[(runtime_caps & simsimd_cap_neon_bf16_k) != 0]);
    printf("- Arm NEON I8 support enabled: %s\n", flags[(runtime_caps & simsimd_cap_neon_i8_k) != 0]);
    printf("- Arm SVE support enabled: %s\n", flags[(runtime_caps & simsimd_cap_sve_k) != 0]);
    printf("- Arm SVE F16 support enabled: %s\n", flags[(runtime_caps & simsimd_cap_sve_f16_k) != 0]);
    printf("- Arm SVE BF16 support enabled: %s\n", flags[(runtime_caps & simsimd_cap_sve_bf16_k) != 0]);
    printf("- Arm SVE I8 support enabled: %s\n", flags[(runtime_caps & simsimd_cap_sve_i8_k) != 0]);
    printf("- Arm SVE2 support enabled: %s\n", flags[(runtime_caps & simsimd_cap_sve2_k) != 0]);
    printf("- x86 Haswell support enabled: %s\n", flags[(runtime_caps & simsimd_cap_haswell_k) != 0]);
    printf("- x86 Skylake support enabled: %s\n", flags[(runtime_caps & simsimd_cap_skylake_k) != 0]);
    printf("- x86 Ice Lake support enabled: %s\n", flags[(runtime_caps & simsimd_cap_ice_k) != 0]);
    printf("- x86 Genoa support enabled: %s\n", flags[(runtime_caps & simsimd_cap_genoa_k) != 0]);
    printf("- x86 Sapphire Rapids support enabled: %s\n", flags[(runtime_caps & simsimd_cap_sapphire_k) != 0]);
    printf("- x86 Turin support enabled: %s\n", flags[(runtime_caps & simsimd_cap_turin_k) != 0]);
    printf("\n");
}

/**
 *  @brief  A trivial test that checks if the utility functions return the expected values.
 */
void test_utilities(void) {
    simsimd_capability_t capabilities = simsimd_capabilities();

    int uses_neon = simsimd_uses_neon();
    int uses_sve = simsimd_uses_sve();
    int uses_haswell = simsimd_uses_haswell();
    int uses_skylake = simsimd_uses_skylake();
    int uses_ice = simsimd_uses_ice();
    int uses_genoa = simsimd_uses_genoa();
    int uses_sapphire = simsimd_uses_sapphire();
    int uses_turin = simsimd_uses_turin();
    int uses_sierra = simsimd_uses_sierra();

    assert(uses_neon == ((capabilities & simsimd_cap_neon_k) != 0));
    assert(uses_sve == ((capabilities & simsimd_cap_sve_k) != 0));
    assert(uses_haswell == ((capabilities & simsimd_cap_haswell_k) != 0));
    assert(uses_skylake == ((capabilities & simsimd_cap_skylake_k) != 0));
    assert(uses_ice == ((capabilities & simsimd_cap_ice_k) != 0));
    assert(uses_genoa == ((capabilities & simsimd_cap_genoa_k) != 0));
    assert(uses_sapphire == ((capabilities & simsimd_cap_sapphire_k) != 0));
    assert(uses_turin == ((capabilities & simsimd_cap_turin_k) != 0));
    assert(uses_sierra == ((capabilities & simsimd_cap_sierra_k) != 0));
}

/**
 *  @brief  A trivial test that calls every implemented distance function and their dispatch versions
 *          on vectors A and B, where A and B are equal.
 */
void test_distance_from_itself(void) {
    simsimd_f64_t f64s[1536];
    simsimd_f32_t f32s[1536];
    simsimd_f16_t f16s[1536];
    simsimd_bf16_t bf16s[1536];

    simsimd_f64c_t f64cs[768];
    simsimd_f32c_t f32cs[768];
    simsimd_f16c_t f16cs[768];
    simsimd_bf16c_t bf16cs[768];

    simsimd_i8_t i8s[1536];
    simsimd_u8_t u8s[1536];
    simsimd_b8_t b8s[1536 / 8];     // 8 bits per word
    simsimd_distance_t distance[2]; // For complex dot-products we need two values

    // Cosine distance between two vectors
    simsimd_cos_i8(i8s, i8s, 1536, &distance[0]);
    simsimd_cos_u8(u8s, u8s, 1536, &distance[0]);
    simsimd_cos_f16(f16s, f16s, 1536, &distance[0]);
    simsimd_cos_bf16(bf16s, bf16s, 1536, &distance[0]);
    simsimd_cos_f32(f32s, f32s, 1536, &distance[0]);
    simsimd_cos_f64(f64s, f64s, 1536, &distance[0]);

    // Euclidean distance between two vectors
    simsimd_l2sq_i8(i8s, i8s, 1536, &distance[0]);
    simsimd_l2sq_u8(u8s, u8s, 1536, &distance[0]);
    simsimd_l2sq_f16(f16s, f16s, 1536, &distance[0]);
    simsimd_l2sq_bf16(bf16s, bf16s, 1536, &distance[0]);
    simsimd_l2sq_f32(f32s, f32s, 1536, &distance[0]);
    simsimd_l2sq_f64(f64s, f64s, 1536, &distance[0]);

    // Inner product between two vectors
    simsimd_dot_i8(i8s, i8s, 1536, &distance[0]);
    simsimd_dot_u8(u8s, u8s, 1536, &distance[0]);
    simsimd_dot_f16(f16s, f16s, 1536, &distance[0]);
    simsimd_dot_bf16(bf16s, bf16s, 1536, &distance[0]);
    simsimd_dot_f32(f32s, f32s, 1536, &distance[0]);
    simsimd_dot_f64(f64s, f64s, 1536, &distance[0]);

    // Complex inner product between two vectors
    simsimd_dot_bf16c(bf16cs, bf16cs, 768, &distance[0]);
    simsimd_dot_f16c(f16cs, f16cs, 768, &distance[0]);
    simsimd_dot_f32c(f32cs, f32cs, 768, &distance[0]);
    simsimd_dot_f64c(f64cs, f64cs, 768, &distance[0]);

    // Complex conjugate inner product between two vectors
    simsimd_vdot_bf16c(bf16cs, bf16cs, 768, &distance[0]);
    simsimd_vdot_f16c(f16cs, f16cs, 768, &distance[0]);
    simsimd_vdot_f32c(f32cs, f32cs, 768, &distance[0]);
    simsimd_vdot_f64c(f64cs, f64cs, 768, &distance[0]);

    // Hamming distance between two vectors
    simsimd_hamming_b8(b8s, b8s, 1536 / 8, &distance[0]);

    // Jaccard distance between two vectors
    simsimd_jaccard_b8(b8s, b8s, 1536 / 8, &distance[0]);

    // Jensen-Shannon divergence between two vectors
    simsimd_js_f16(f16s, f16s, 1536, &distance[0]);
    simsimd_js_bf16(bf16s, bf16s, 1536, &distance[0]);
    simsimd_js_f32(f32s, f32s, 1536, &distance[0]);
    simsimd_js_f64(f64s, f64s, 1536, &distance[0]);

    // Kullback-Leibler divergence between two vectors
    simsimd_kl_f16(f16s, f16s, 1536, &distance[0]);
    simsimd_kl_bf16(bf16s, bf16s, 1536, &distance[0]);
    simsimd_kl_f32(f32s, f32s, 1536, &distance[0]);
    simsimd_kl_f64(f64s, f64s, 1536, &distance[0]);
}

int main(int argc, char **argv) {

    print_capabilities();
    test_utilities();
    test_distance_from_itself();
    return 0;
}
