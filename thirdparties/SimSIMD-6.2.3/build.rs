fn main() {
    let mut build = cc::Build::new();

    build
        .file("c/lib.c")
        .include("include")
        .define("SIMSIMD_NATIVE_F16", "0")
        .define("SIMSIMD_NATIVE_BF16", "0")
        .define("SIMSIMD_DYNAMIC_DISPATCH", "1")
        .flag("-O3")
        .flag("-std=c99") // Enforce C99 standard
        .flag("-pedantic") // Ensure strict compliance with the C standard
        .warnings(false);

    if build.try_compile("simsimd").is_err() {
        print!("cargo:warning=Failed to compile with all SIMD backends...");

        let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
        let flags_to_try = match target_arch.as_str() {
            "arm" | "aarch64" => vec![
                "SIMSIMD_TARGET_SVE2",
                "SIMSIMD_TARGET_SVE_BF16",
                "SIMSIMD_TARGET_SVE_F16",
                "SIMSIMD_TARGET_SVE_I8",
                "SIMSIMD_TARGET_SVE",
                "SIMSIMD_TARGET_NEON_BF16",
                "SIMSIMD_TARGET_NEON_F16",
                "SIMSIMD_TARGET_NEON_I8",
                "SIMSIMD_TARGET_NEON",
            ],
            _ => vec![
                "SIMSIMD_TARGET_SIERRA",
                "SIMSIMD_TARGET_TURIN",
                "SIMSIMD_TARGET_SAPPHIRE",
                "SIMSIMD_TARGET_GENOA",
                "SIMSIMD_TARGET_ICE",
                "SIMSIMD_TARGET_SKYLAKE",
                "SIMSIMD_TARGET_HASWELL",
            ],
        };

        for flag in flags_to_try.iter() {
            build.define(flag, "0");
            if build.try_compile("simsimd").is_ok() {
                break;
            }

            // Print the failed configuration
            println!(
                "cargo:warning=Failed to compile after disabling {}, trying next configuration...",
                flag
            );
        }
    }

    println!("cargo:rerun-if-changed=c/lib.c");
    println!("cargo:rerun-if-changed=rust/lib.rs");
    println!("cargo:rerun-if-changed=include/simsimd/simsimd.h");

    println!("cargo:rerun-if-changed=include/simsimd/dot.h");
    println!("cargo:rerun-if-changed=include/simsimd/spatial.h");
    println!("cargo:rerun-if-changed=include/simsimd/probability.h");
    println!("cargo:rerun-if-changed=include/simsimd/binary.h");
    println!("cargo:rerun-if-changed=include/simsimd/types.h");
}
