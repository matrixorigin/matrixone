//! # SpatialSimilarity - Hardware-Accelerated Similarity Metrics and Distance Functions
//!
//! * Targets ARM NEON, SVE, x86 AVX2, AVX-512 (VNNI, FP16) hardware backends.
//! * Handles `f64` double-, `f32` single-, and `f16` half-precision, `i8` integral, and binary vectors.
//! * Zero-dependency header-only C 99 library with bindings for Rust and other langauges.
//!
//! ## Implemented distance functions include:
//!
//! * Euclidean (L2), Inner Distance, and Cosine (Angular) spatial distances.
//! * Hamming (~ Manhattan) and Jaccard (~ Tanimoto) binary distances.
//! * Kullback-Leibler and Jensen-Shannon divergences for probability distributions.
//!
//! ## Example
//!
//! ```rust
//! use simsimd::SpatialSimilarity;
//!
//! let a = &[1, 2, 3];
//! let b = &[4, 5, 6];
//!
//! // Compute cosine similarity
//! let cos_sim = i8::cos(a, b);
//!
//! // Compute dot product distance
//! let dot_product = i8::dot(a, b);
//!
//! // Compute squared Euclidean distance
//! let l2sq_dist = i8::l2sq(a, b);
//! ```
//!
//! ## Traits
//!
//! The `SpatialSimilarity` trait covers following methods:
//!
//! - `cosine(a: &[Self], b: &[Self]) -> Option<Distance>`: Computes cosine similarity between two slices.
//! - `dot(a: &[Self], b: &[Self]) -> Option<Distance>`: Computes dot product distance between two slices.
//! - `sqeuclidean(a: &[Self], b: &[Self]) -> Option<Distance>`: Computes squared Euclidean distance between two slices.
//!
//! The `BinarySimilarity` trait covers following methods:
//!
//! - `hamming(a: &[Self], b: &[Self]) -> Option<Distance>`: Computes Hamming distance between two slices.
//! - `jaccard(a: &[Self], b: &[Self]) -> Option<Distance>`: Computes Jaccard index between two slices.
//!
//! The `ProbabilitySimilarity` trait covers following methods:
//!
//! - `jensenshannon(a: &[Self], b: &[Self]) -> Option<Distance>`: Computes Jensen-Shannon divergence between two slices.
//! - `kullbackleibler(a: &[Self], b: &[Self]) -> Option<Distance>`: Computes Kullback-Leibler divergence between two slices.
//!
#![allow(non_camel_case_types)]

type Distance = f64;
type ComplexProduct = (f64, f64);

extern "C" {

    fn simsimd_dot_f16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_dot_bf16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_dot_f32(a: *const f32, b: *const f32, c: usize, d: *mut Distance);
    fn simsimd_dot_f64(a: *const f64, b: *const f64, c: usize, d: *mut Distance);

    fn simsimd_dot_f16c(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_dot_bf16c(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_dot_f32c(a: *const f32, b: *const f32, c: usize, d: *mut Distance);
    fn simsimd_dot_f64c(a: *const f64, b: *const f64, c: usize, d: *mut Distance);

    fn simsimd_vdot_f16c(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_vdot_bf16c(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_vdot_f32c(a: *const f32, b: *const f32, c: usize, d: *mut Distance);
    fn simsimd_vdot_f64c(a: *const f64, b: *const f64, c: usize, d: *mut Distance);

    fn simsimd_cos_i8(a: *const i8, b: *const i8, c: usize, d: *mut Distance);
    fn simsimd_cos_f16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_cos_bf16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_cos_f32(a: *const f32, b: *const f32, c: usize, d: *mut Distance);
    fn simsimd_cos_f64(a: *const f64, b: *const f64, c: usize, d: *mut Distance);

    fn simsimd_l2sq_i8(a: *const i8, b: *const i8, c: usize, d: *mut Distance);
    fn simsimd_l2sq_f16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_l2sq_bf16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_l2sq_f32(a: *const f32, b: *const f32, c: usize, d: *mut Distance);
    fn simsimd_l2sq_f64(a: *const f64, b: *const f64, c: usize, d: *mut Distance);

    fn simsimd_l2_i8(a: *const i8, b: *const i8, c: usize, d: *mut Distance);
    fn simsimd_l2_f16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_l2_bf16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_l2_f32(a: *const f32, b: *const f32, c: usize, d: *mut Distance);
    fn simsimd_l2_f64(a: *const f64, b: *const f64, c: usize, d: *mut Distance);

    fn simsimd_hamming_b8(a: *const u8, b: *const u8, c: usize, d: *mut Distance);
    fn simsimd_jaccard_b8(a: *const u8, b: *const u8, c: usize, d: *mut Distance);

    fn simsimd_js_f16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_js_bf16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_js_f32(a: *const f32, b: *const f32, c: usize, d: *mut Distance);
    fn simsimd_js_f64(a: *const f64, b: *const f64, c: usize, d: *mut Distance);

    fn simsimd_kl_f16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_kl_bf16(a: *const u16, b: *const u16, c: usize, d: *mut Distance);
    fn simsimd_kl_f32(a: *const f32, b: *const f32, c: usize, d: *mut Distance);
    fn simsimd_kl_f64(a: *const f64, b: *const f64, c: usize, d: *mut Distance);

    fn simsimd_intersect_u16(a: *const u16, b: *const u16, a_length: usize, b_length: usize, d: *mut Distance);
    fn simsimd_intersect_u32(a: *const u32, b: *const u32, a_length: usize, b_length: usize, d: *mut Distance);

    fn simsimd_uses_neon() -> i32;
    fn simsimd_uses_neon_f16() -> i32;
    fn simsimd_uses_neon_bf16() -> i32;
    fn simsimd_uses_neon_i8() -> i32;
    fn simsimd_uses_sve() -> i32;
    fn simsimd_uses_sve_f16() -> i32;
    fn simsimd_uses_sve_bf16() -> i32;
    fn simsimd_uses_sve_i8() -> i32;
    fn simsimd_uses_haswell() -> i32;
    fn simsimd_uses_skylake() -> i32;
    fn simsimd_uses_ice() -> i32;
    fn simsimd_uses_genoa() -> i32;
    fn simsimd_uses_sapphire() -> i32;
    fn simsimd_uses_turin() -> i32;
    fn simsimd_uses_sierra() -> i32;
}

/// A half-precision floating point number.
#[repr(transparent)]
pub struct f16(u16);

impl f16 {}

/// A half-precision floating point number, called brain float.
#[repr(transparent)]
pub struct bf16(u16);

impl bf16 {}

/// The `capabilities` module provides functions for detecting the hardware features
/// available on the current system.
pub mod capabilities {

    pub fn uses_neon() -> bool {
        unsafe { crate::simsimd_uses_neon() != 0 }
    }

    pub fn uses_neon_f16() -> bool {
        unsafe { crate::simsimd_uses_neon_f16() != 0 }
    }

    pub fn uses_neon_bf16() -> bool {
        unsafe { crate::simsimd_uses_neon_bf16() != 0 }
    }

    pub fn uses_neon_i8() -> bool {
        unsafe { crate::simsimd_uses_neon_i8() != 0 }
    }

    pub fn uses_sve() -> bool {
        unsafe { crate::simsimd_uses_sve() != 0 }
    }

    pub fn uses_sve_f16() -> bool {
        unsafe { crate::simsimd_uses_sve_f16() != 0 }
    }

    pub fn uses_sve_bf16() -> bool {
        unsafe { crate::simsimd_uses_sve_bf16() != 0 }
    }

    pub fn uses_sve_i8() -> bool {
        unsafe { crate::simsimd_uses_sve_i8() != 0 }
    }

    pub fn uses_haswell() -> bool {
        unsafe { crate::simsimd_uses_haswell() != 0 }
    }

    pub fn uses_skylake() -> bool {
        unsafe { crate::simsimd_uses_skylake() != 0 }
    }

    pub fn uses_ice() -> bool {
        unsafe { crate::simsimd_uses_ice() != 0 }
    }

    pub fn uses_genoa() -> bool {
        unsafe { crate::simsimd_uses_genoa() != 0 }
    }

    pub fn uses_sapphire() -> bool {
        unsafe { crate::simsimd_uses_sapphire() != 0 }
    }

    pub fn uses_turin() -> bool {
        unsafe { crate::simsimd_uses_turin() != 0 }
    }

    pub fn uses_sierra() -> bool {
        unsafe { crate::simsimd_uses_sierra() != 0 }
    }
}

/// `SpatialSimilarity` provides a set of trait methods for computing similarity
/// or distance between spatial data vectors in SIMD (Single Instruction, Multiple Data) context.
/// These methods can be used to calculate metrics like cosine similarity, dot product,
/// and squared Euclidean distance between two slices of data.
///
/// Each method takes two slices of data (a and b) and returns an Option<Distance>.
/// The result is `None` if the slices are not of the same length, as these operations
/// require one-to-one correspondence between the elements of the slices.
/// Otherwise, it returns the computed similarity or distance as `Some(f32)`.
pub trait SpatialSimilarity
where
    Self: Sized,
{
    /// Computes the cosine similarity between two slices.
    /// The cosine similarity is a measure of similarity between two non-zero vectors
    /// of an dot product space that measures the cosine of the angle between them.
    fn cos(a: &[Self], b: &[Self]) -> Option<Distance>;

    /// Computes the inner product (also known as dot product) between two slices.
    /// The dot product is the sum of the products of the corresponding entries
    /// of the two sequences of numbers.
    fn dot(a: &[Self], b: &[Self]) -> Option<Distance>;

    /// Computes the squared Euclidean distance between two slices.
    /// The squared Euclidean distance is the sum of the squared differences
    /// between corresponding elements of the two slices.
    fn l2sq(a: &[Self], b: &[Self]) -> Option<Distance>;

    /// Computes the Euclidean distance between two slices.
    /// The Euclidean distance is the square root of 
    //  sum of the squared differences between corresponding 
    /// elements of the two slices.
    fn l2(a: &[Self], b: &[Self]) -> Option<Distance>;

    /// Computes the squared Euclidean distance between two slices.
    /// The squared Euclidean distance is the sum of the squared differences
    /// between corresponding elements of the two slices.
    fn sqeuclidean(a: &[Self], b: &[Self]) -> Option<Distance> {
        SpatialSimilarity::l2sq(a, b)
    }

    /// Computes the Euclidean distance between two slices.
    /// The Euclidean distance is the square root of the 
    /// sum of the squared differences between corresponding 
    /// elements of the two slices.
    fn euclidean(a: &[Self], b: &[Self]) -> Option<Distance> {
        SpatialSimilarity::l2(a, b)
    }

    /// Computes the squared Euclidean distance between two slices.
    /// The squared Euclidean distance is the sum of the squared differences
    /// between corresponding elements of the two slices.
    fn inner(a: &[Self], b: &[Self]) -> Option<Distance> {
        SpatialSimilarity::dot(a, b)
    }

    /// Computes the cosine similarity between two slices.
    /// The cosine similarity is a measure of similarity between two non-zero vectors
    /// of an dot product space that measures the cosine of the angle between them.
    fn cosine(a: &[Self], b: &[Self]) -> Option<Distance> {
        SpatialSimilarity::cos(a, b)
    }
}

/// `BinarySimilarity` provides trait methods for computing similarity metrics
/// that are commonly used with binary data vectors, such as Hamming distance
/// and Jaccard index.
///
/// The methods accept two slices of binary data and return an Option<Distance>
/// indicating the computed similarity or distance, with `None` returned if the
/// slices differ in length.
pub trait BinarySimilarity
where
    Self: Sized,
{
    /// Computes the Hamming distance between two binary data slices.
    /// The Hamming distance between two strings of equal length is the number of
    /// bits at which the corresponding values are different.
    fn hamming(a: &[Self], b: &[Self]) -> Option<Distance>;

    /// Computes the Jaccard index between two bitsets represented by binary data slices.
    /// The Jaccard index, also known as the Jaccard similarity coefficient, is a statistic
    /// used for gauging the similarity and diversity of sample sets.
    fn jaccard(a: &[Self], b: &[Self]) -> Option<Distance>;
}

/// `ProbabilitySimilarity` provides trait methods for computing similarity or divergence
/// measures between probability distributions, such as the Jensen-Shannon divergence
/// and the Kullback-Leibler divergence.
///
/// These methods are particularly useful in contexts such as information theory and
/// machine learning, where one often needs to measure how one probability distribution
/// differs from a second, reference probability distribution.
pub trait ProbabilitySimilarity
where
    Self: Sized,
{
    /// Computes the Jensen-Shannon divergence between two probability distributions.
    /// The Jensen-Shannon divergence is a method of measuring the similarity between
    /// two probability distributions. It is based on the Kullback-Leibler divergence,
    /// but is symmetric and always has a finite value.
    fn jensenshannon(a: &[Self], b: &[Self]) -> Option<Distance>;

    /// Computes the Kullback-Leibler divergence between two probability distributions.
    /// The Kullback-Leibler divergence is a measure of how one probability distribution
    /// diverges from a second, expected probability distribution.
    fn kullbackleibler(a: &[Self], b: &[Self]) -> Option<Distance>;
}

/// `ComplexProducts` provides trait methods for computing products between
/// complex number vectors. This includes standard and Hermitian dot products.
pub trait ComplexProducts
where
    Self: Sized,
{
    /// Computes the dot product between two complex number vectors.
    fn dot(a: &[Self], b: &[Self]) -> Option<ComplexProduct>;

    /// Computes the Hermitian dot product (conjugate dot product) between two complex number vectors.
    fn vdot(a: &[Self], b: &[Self]) -> Option<ComplexProduct>;
}

/// `Sparse` provides trait methods for sparse vectors.
pub trait Sparse
where
    Self: Sized,
{
    /// Computes the number of common elements between two sparse vectors.
    /// both vectors must be sorted in ascending order.
    fn intersect(a: &[Self], b: &[Self]) -> Option<Distance>;
}

impl BinarySimilarity for u8 {
    fn hamming(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_hamming_b8(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn jaccard(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_jaccard_b8(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl SpatialSimilarity for i8 {
    fn cos(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_cos_i8(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn dot(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_cos_i8(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2sq(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2sq_i8(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2_i8(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl Sparse for u16 {
      
    fn intersect(a: &[Self], b: &[Self]) -> Option<Distance> {
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_intersect_u16(a.as_ptr(), b.as_ptr(), a.len(), b.len(), distance_ptr) };
        Some(distance_value)
    }

}

impl Sparse for u32 {
      
    fn intersect(a: &[Self], b: &[Self]) -> Option<Distance> {
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_intersect_u32(a.as_ptr(), b.as_ptr(), a.len(), b.len(), distance_ptr) };
        Some(distance_value)
    }

}

impl SpatialSimilarity for f16 {
    fn cos(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const f16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_cos_f16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn dot(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const f16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_dot_f16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2sq(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const f16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2sq_f16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2(a: &[Self], b: &[Self]) -> Option<Distance> {
        
        if a.len() != b.len() {
            return None;
        }
        // Explicitly cast `*const f16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2_f16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl SpatialSimilarity for bf16 {
    fn cos(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const bf16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_cos_bf16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn dot(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const bf16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_dot_bf16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2sq(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const bf16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2sq_bf16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        // Explicitly cast `*const bf16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2sq_bf16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl SpatialSimilarity for f32 {
    fn cos(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_cos_f32(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn dot(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_dot_f32(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2sq(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2sq_f32(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2_f32(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl SpatialSimilarity for f64 {
    fn cos(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_cos_f64(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn dot(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_dot_f64(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2sq(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2sq_f64(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn l2(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_l2_f64(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl ProbabilitySimilarity for f16 {
    fn jensenshannon(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const f16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_js_f16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn kullbackleibler(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const f16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_kl_f16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl ProbabilitySimilarity for bf16 {
    fn jensenshannon(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const bf16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_js_bf16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn kullbackleibler(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }

        // Explicitly cast `*const bf16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_kl_bf16(a_ptr, b_ptr, a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl ProbabilitySimilarity for f32 {
    fn jensenshannon(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_js_f32(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn kullbackleibler(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_kl_f32(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl ProbabilitySimilarity for f64 {
    fn jensenshannon(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_js_f64(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }

    fn kullbackleibler(a: &[Self], b: &[Self]) -> Option<Distance> {
        if a.len() != b.len() {
            return None;
        }
        let mut distance_value: Distance = 0.0;
        let distance_ptr: *mut Distance = &mut distance_value as *mut Distance;
        unsafe { simsimd_kl_f64(a.as_ptr(), b.as_ptr(), a.len(), distance_ptr) };
        Some(distance_value)
    }
}

impl ComplexProducts for f16 {
    fn dot(a: &[Self], b: &[Self]) -> Option<ComplexProduct> {
        if a.len() != b.len() {
            return None;
        }
        // Prepare the output array where the real and imaginary parts will be stored
        let mut product: [Distance; 2] = [0.0, 0.0];
        let product_ptr: *mut Distance = &mut product[0] as *mut _;
        // Explicitly cast `*const f16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        unsafe { simsimd_dot_f16c(a_ptr, b_ptr, a.len(), product_ptr) };
        Some((product[0], product[1]))
    }

    fn vdot(a: &[Self], b: &[Self]) -> Option<ComplexProduct> {
        if a.len() != b.len() {
            return None;
        }
        let mut product: [Distance; 2] = [0.0, 0.0];
        let product_ptr: *mut Distance = &mut product[0] as *mut _;
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        unsafe { simsimd_vdot_f16c(a_ptr, b_ptr, a.len(), product_ptr) };
        Some((product[0], product[1]))
    }
}

impl ComplexProducts for bf16 {
    fn dot(a: &[Self], b: &[Self]) -> Option<ComplexProduct> {
        if a.len() != b.len() {
            return None;
        }
        // Prepare the output array where the real and imaginary parts will be stored
        let mut product: [Distance; 2] = [0.0, 0.0];
        let product_ptr: *mut Distance = &mut product[0] as *mut _;
        // Explicitly cast `*const bf16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        unsafe { simsimd_dot_bf16c(a_ptr, b_ptr, a.len(), product_ptr) };
        Some((product[0], product[1]))
    }

    fn vdot(a: &[Self], b: &[Self]) -> Option<ComplexProduct> {
        if a.len() != b.len() {
            return None;
        }
        // Prepare the output array where the real and imaginary parts will be stored
        let mut product: [Distance; 2] = [0.0, 0.0];
        let product_ptr: *mut Distance = &mut product[0] as *mut _;
        // Explicitly cast `*const bf16` to `*const u16`
        let a_ptr = a.as_ptr() as *const u16;
        let b_ptr = b.as_ptr() as *const u16;
        unsafe { simsimd_vdot_bf16c(a_ptr, b_ptr, a.len(), product_ptr) };
        Some((product[0], product[1]))
    }
}

impl ComplexProducts for f32 {
    fn dot(a: &[Self], b: &[Self]) -> Option<ComplexProduct> {
        if a.len() != b.len() {
            return None;
        }
        let mut product: [Distance; 2] = [0.0, 0.0];
        let product_ptr: *mut Distance = &mut product[0] as *mut _;
        unsafe { simsimd_dot_f32c(a.as_ptr(), b.as_ptr(), a.len(), product_ptr) };
        Some((product[0], product[1]))
    }

    fn vdot(a: &[Self], b: &[Self]) -> Option<ComplexProduct> {
        if a.len() != b.len() {
            return None;
        }
        let mut product: [Distance; 2] = [0.0, 0.0];
        let product_ptr: *mut Distance = &mut product[0] as *mut _;
        unsafe { simsimd_vdot_f32c(a.as_ptr(), b.as_ptr(), a.len(), product_ptr) };
        Some((product[0], product[1]))
    }
}

impl ComplexProducts for f64 {
    fn dot(a: &[Self], b: &[Self]) -> Option<ComplexProduct> {
        if a.len() != b.len() {
            return None;
        }
        let mut product: [Distance; 2] = [0.0, 0.0];
        let product_ptr: *mut Distance = &mut product[0] as *mut _;
        unsafe { simsimd_dot_f64c(a.as_ptr(), b.as_ptr(), a.len(), product_ptr) };
        Some((product[0], product[1]))
    }

    fn vdot(a: &[Self], b: &[Self]) -> Option<ComplexProduct> {
        if a.len() != b.len() {
            return None;
        }
        let mut product: [Distance; 2] = [0.0, 0.0];
        let product_ptr: *mut Distance = &mut product[0] as *mut _;
        unsafe { simsimd_vdot_f64c(a.as_ptr(), b.as_ptr(), a.len(), product_ptr) };
        Some((product[0], product[1]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use half::bf16 as HalfBF16;
    use half::f16 as HalfF16;

    #[test]
    fn test_hardware_features_detection() {
        let uses_arm = capabilities::uses_neon() || capabilities::uses_sve();
        let uses_x86 = capabilities::uses_haswell()
            || capabilities::uses_skylake()
            || capabilities::uses_ice()
            || capabilities::uses_genoa()
            || capabilities::uses_sapphire()
            || capabilities::uses_turin();

        // The CPU can't simultaneously support ARM and x86 SIMD extensions
        if uses_arm {
            assert!(!uses_x86);
        }
        if uses_x86 {
            assert!(!uses_arm);
        }

        println!("- uses_neon: {}", capabilities::uses_neon());
        println!("- uses_sve: {}", capabilities::uses_sve());
        println!("- uses_haswell: {}", capabilities::uses_haswell());
        println!("- uses_skylake: {}", capabilities::uses_skylake());
        println!("- uses_ice: {}", capabilities::uses_ice());
        println!("- uses_genoa: {}", capabilities::uses_genoa());
        println!("- uses_sapphire: {}", capabilities::uses_sapphire());
        println!("- uses_turin: {}", capabilities::uses_turin());
        println!("- uses_sierra: {}", capabilities::uses_sierra());
    }

    //
    fn assert_almost_equal(left: Distance, right: Distance, tolerance: Distance) {
        let lower = right - tolerance;
        let upper = right + tolerance;

        assert!(left >= lower && left <= upper);
    }

    #[test]
    fn test_cos_i8() {
        let a = &[3, 97, 127];
        let b = &[3, 97, 127];

        if let Some(result) = SpatialSimilarity::cosine(a, b) {
            println!("The result of cos_i8 is {:.8}", result);
            assert_almost_equal(0.00012027938, result, 0.01);
        }
    }

    #[test]
    fn test_cos_f32() {
        let a = &[1.0, 2.0, 3.0];
        let b = &[4.0, 5.0, 6.0];

        if let Some(result) = SpatialSimilarity::cosine(a, b) {
            println!("The result of cos_f32 is {:.8}", result);
            assert_almost_equal(0.025, result, 0.01);
        }
    }

    #[test]
    fn test_dot_i8() {
        let a = &[1, 2, 3];
        let b = &[4, 5, 6];

        if let Some(result) = SpatialSimilarity::dot(a, b) {
            println!("The result of dot_i8 is {:.8}", result);
            assert_almost_equal(0.029403687, result, 0.01);
        }
    }

    #[test]
    fn test_dot_f32() {
        let a = &[1.0, 2.0, 3.0];
        let b = &[4.0, 5.0, 6.0];

        if let Some(result) = SpatialSimilarity::dot(a, b) {
            println!("The result of dot_f32 is {:.8}", result);
            assert_almost_equal(32.0, result, 0.01);
        }
    }

    #[test]
    fn test_dot_f32_complex() {
        // Let's consider these as complex numbers where every pair is (real, imaginary)
        let a: &[f32; 4] = &[1.0, 2.0, 3.0, 4.0]; // Represents two complex numbers: 1+2i, 3+4i
        let b: &[f32; 4] = &[5.0, 6.0, 7.0, 8.0]; // Represents two complex numbers: 5+6i, 7+8i

        if let Some((real, imag)) = ComplexProducts::dot(a, b) {
            println!(
                "The result of dot_f32_complex is real: {:.8}, imag: {:.8}",
                real, imag
            );
            // These values should be replaced with the expected real and imaginary parts of the result
            assert_almost_equal(-18.0, real, 0.01); // Corrected expected real part
            assert_almost_equal(68.0, imag, 0.01); // Corrected expected imaginary part
        }
    }

    #[test]
    fn test_vdot_f32_complex() {
        // Here we're assuming a similar setup to the previous test, but for the Hermitian (conjugate) dot product
        let a: &[f32; 4] = &[1.0, 2.0, 3.0, 4.0]; // Represents two complex numbers: 1+2i, 3+4i
        let b: &[f32; 4] = &[5.0, 6.0, 7.0, 8.0]; // Represents two complex numbers: 5+6i, 7+8i

        if let Some((real, imag)) = ComplexProducts::vdot(a, b) {
            println!(
                "The result of vdot_f32_complex is real: {:.8}, imag: {:.8}",
                real, imag
            );
            // Replace these with the actual expected values
            assert_almost_equal(70.0, real, 0.01); // Example expected real part
            assert_almost_equal(-8.0, imag, 0.01); // Example expected imaginary part
        }
    }

    #[test]
    fn test_l2sq_i8() {
        let a = &[1, 2, 3];
        let b = &[4, 5, 6];

        if let Some(result) = SpatialSimilarity::sqeuclidean(a, b) {
            println!("The result of l2sq_i8 is {:.8}", result);
            assert_almost_equal(27.0, result, 0.01);
        }
    }

    #[test]
    fn test_l2sq_f32() {
        let a = &[1.0, 2.0, 3.0];
        let b = &[4.0, 5.0, 6.0];

        if let Some(result) = SpatialSimilarity::sqeuclidean(a, b) {
            println!("The result of l2sq_f32 is {:.8}", result);
            assert_almost_equal(27.0, result, 0.01);
        }
    }

    #[test]
    fn test_l2_f32() {
        let a: &[f32; 3] = &[1.0, 2.0, 3.0];
        let b: &[f32; 3] = &[4.0, 5.0, 6.0];
        if let Some(result) = SpatialSimilarity::euclidean(a, b) {
            println!("The result of l2_f32 is {:.8}", result);
            assert_almost_equal(5.2, result, 0.01);
        }
    }

    #[test]
    fn test_l2_f64() {
        let a: &[f64; 3] = &[1.0, 2.0, 3.0];
        let b: &[f64; 3] = &[4.0, 5.0, 6.0];
        if let Some(result) = SpatialSimilarity::euclidean(a, b) {
            println!("The result of l2_f64 is {:.8}", result);
            assert_almost_equal(5.2, result, 0.01);
        }
    }

    #[test]
    fn test_l2_f16() {
        let a_half: Vec<HalfF16> = vec![1.0, 2.0, 3.0]
            .iter()
            .map(|&x| HalfF16::from_f32(x))
            .collect();
        let b_half: Vec<HalfF16> = vec![4.0, 5.0, 6.0]
            .iter()
            .map(|&x| HalfF16::from_f32(x))
            .collect();

        
        let a_simsimd: &[f16] =
            unsafe { std::slice::from_raw_parts(a_half.as_ptr() as *const f16, a_half.len()) };
        let b_simsimd: &[f16] =
            unsafe { std::slice::from_raw_parts(b_half.as_ptr() as *const f16, b_half.len()) };

        if let Some(result) = SpatialSimilarity::euclidean(&a_simsimd, &b_simsimd) {
            println!("The result of l2_f16 is {:.8}", result);
            assert_almost_equal(5.2, result, 0.01);
        }
        
    }

    #[test]
    fn test_l2_i8() {
        let a = &[1, 2, 3];
        let b = &[4, 5, 6];

        if let Some(result) = SpatialSimilarity::euclidean(a, b) {
            println!("The result of l2_i8 is {:.8}", result);
            assert_almost_equal(5.2, result, 0.01);
        }
    }
    // Adding new tests for bit-level distances
    #[test]
    fn test_hamming_u8() {
        let a = &[0b01010101, 0b11110000, 0b10101010]; // Binary representations for clarity
        let b = &[0b01010101, 0b11110000, 0b10101010];

        if let Some(result) = BinarySimilarity::hamming(a, b) {
            println!("The result of hamming_u8 is {:.8}", result);
            assert_almost_equal(0.0, result, 0.01); // Perfect match
        }
    }

    #[test]
    fn test_jaccard_u8() {
        // For binary data, treat each byte as a set of bits
        let a = &[0b11110000, 0b00001111, 0b10101010];
        let b = &[0b11110000, 0b00001111, 0b01010101];

        if let Some(result) = BinarySimilarity::jaccard(a, b) {
            println!("The result of jaccard_u8 is {:.8}", result);
            assert_almost_equal(0.5, result, 0.01); // Example value
        }
    }

    // Adding new tests for probability similarities
    #[test]
    fn test_js_f32() {
        let a: &[f32; 3] = &[0.1, 0.9, 0.0];
        let b: &[f32; 3] = &[0.2, 0.8, 0.0];

        if let Some(result) = ProbabilitySimilarity::jensenshannon(a, b) {
            println!("The result of js_f32 is {:.8}", result);
            assert_almost_equal(0.099, result, 0.01); // Example value
        }
    }

    #[test]
    fn test_kl_f32() {
        let a: &[f32; 3] = &[0.1, 0.9, 0.0];
        let b: &[f32; 3] = &[0.2, 0.8, 0.0];

        if let Some(result) = ProbabilitySimilarity::kullbackleibler(a, b) {
            println!("The result of kl_f32 is {:.8}", result);
            assert_almost_equal(0.036, result, 0.01); // Example value
        }
    }

    #[test]
    fn test_cos_f16_same() {
        // Assuming these u16 values represent f16 bit patterns, and they are identical
        let a_u16: &[u16] = &[15360, 16384, 17408]; // Corresponding to some f16 values
        let b_u16: &[u16] = &[15360, 16384, 17408]; // Same as above for simplicity

        // Reinterpret cast from &[u16] to &[f16]
        // SAFETY: This is safe as long as the representations are guaranteed to be identical,
        // which they are for transparent structs wrapping the same type.
        let a_f16: &[f16] =
            unsafe { std::slice::from_raw_parts(a_u16.as_ptr() as *const f16, a_u16.len()) };
        let b_f16: &[f16] =
            unsafe { std::slice::from_raw_parts(b_u16.as_ptr() as *const f16, b_u16.len()) };

        if let Some(result) = SpatialSimilarity::cosine(a_f16, b_f16) {
            println!("The result of cos_f16 is {:.8}", result);
            assert_almost_equal(0.0, result, 0.01); // Example value, adjust according to actual expected value
        }
    }

    #[test]
    fn test_cos_bf16_same() {
        // Assuming these u16 values represent bf16 bit patterns, and they are identical
        let a_u16: &[u16] = &[15360, 16384, 17408]; // Corresponding to some bf16 values
        let b_u16: &[u16] = &[15360, 16384, 17408]; // Same as above for simplicity

        // Reinterpret cast from &[u16] to &[bf16]
        // SAFETY: This is safe as long as the representations are guaranteed to be identical,
        // which they are for transparent structs wrapping the same type.
        let a_bf16: &[bf16] =
            unsafe { std::slice::from_raw_parts(a_u16.as_ptr() as *const bf16, a_u16.len()) };
        let b_bf16: &[bf16] =
            unsafe { std::slice::from_raw_parts(b_u16.as_ptr() as *const bf16, b_u16.len()) };

        if let Some(result) = SpatialSimilarity::cosine(a_bf16, b_bf16) {
            println!("The result of cos_bf16 is {:.8}", result);
            assert_almost_equal(0.0, result, 0.01); // Example value, adjust according to actual expected value
        }
    }

    #[test]
    fn test_cos_f16_interop() {
        let a_half: Vec<HalfF16> = vec![1.0, 2.0, 3.0]
            .iter()
            .map(|&x| HalfF16::from_f32(x))
            .collect();
        let b_half: Vec<HalfF16> = vec![4.0, 5.0, 6.0]
            .iter()
            .map(|&x| HalfF16::from_f32(x))
            .collect();

        // SAFETY: This is safe as long as the memory representations are guaranteed to be identical,
        // which they are due to both being #[repr(transparent)] wrappers around u16.
        let a_simsimd: &[f16] =
            unsafe { std::slice::from_raw_parts(a_half.as_ptr() as *const f16, a_half.len()) };
        let b_simsimd: &[f16] =
            unsafe { std::slice::from_raw_parts(b_half.as_ptr() as *const f16, b_half.len()) };

        // Use the reinterpret-casted slices with your SpatialSimilarity implementation
        if let Some(result) = SpatialSimilarity::cosine(a_simsimd, b_simsimd) {
            // Expected value might need adjustment depending on actual cosine functionality
            // Assuming identical vectors yield cosine similarity of 1.0
            println!("The result of cos_f16 (interop) is {:.8}", result);
            assert_almost_equal(0.025, result, 0.01);
        }
    }

    #[test]
    fn test_cos_bf16_interop() {
        let a_half: Vec<HalfBF16> = vec![1.0, 2.0, 3.0]
            .iter()
            .map(|&x| HalfBF16::from_f32(x))
            .collect();
        let b_half: Vec<HalfBF16> = vec![4.0, 5.0, 6.0]
            .iter()
            .map(|&x| HalfBF16::from_f32(x))
            .collect();

        // SAFETY: This is safe as long as the memory representations are guaranteed to be identical,
        // which they are due to both being #[repr(transparent)] wrappers around u16.
        let a_simsimd: &[bf16] =
            unsafe { std::slice::from_raw_parts(a_half.as_ptr() as *const bf16, a_half.len()) };
        let b_simsimd: &[bf16] =
            unsafe { std::slice::from_raw_parts(b_half.as_ptr() as *const bf16, b_half.len()) };

        // Use the reinterpret-casted slices with your SpatialSimilarity implementation
        if let Some(result) = SpatialSimilarity::cosine(a_simsimd, b_simsimd) {
            // Expected value might need adjustment depending on actual cosine functionality
            // Assuming identical vectors yield cosine similarity of 1.0
            println!("The result of cos_bf16 (interop) is {:.8}", result);
            assert_almost_equal(0.025, result, 0.01);
        }
    }

    #[test]
    fn test_intersect_u16() {
        {
            let a_u16: &[u16] = &[153, 16384, 17408]; 
            let b_u16: &[u16] = &[15360, 16384, 7408]; 

            if let Some(result) = Sparse::intersect(a_u16, b_u16) {
                println!("The result of intersect_u16 is {:.8}", result);
                assert_almost_equal(1.0, result, 0.0001);
            }
        }

        {
            let a_u16: &[u16] = &[153, 11638, 08]; 
            let b_u16: &[u16] = &[15360, 16384, 7408]; 

            if let Some(result) = Sparse::intersect(a_u16, b_u16) {
                println!("The result of intersect_u16 is {:.8}", result);
                assert_almost_equal(0.0, result, 0.0001);
            }   
        }
    }

    #[test]
    fn test_intersect_u32() {
        {
            let a_u32: &[u32] = &[11, 153]; 
            let b_u32: &[u32] = &[11, 153, 7408, 16384]; 

            if let Some(result) = Sparse::intersect(a_u32, b_u32) {
                println!("The result of intersect_u32 is {:.8}", result);
                assert_almost_equal(2.0, result, 0.0001);
            }
        }
        
        {
            let a_u32: &[u32] = &[153, 7408, 11638]; 
            let b_u32: &[u32] = &[153, 7408, 11638]; 

            if let Some(result) = Sparse::intersect(a_u32, b_u32) {
                println!("The result of intersect_u32 is {:.8}", result);
                assert_almost_equal(3.0, result, 0.0001);
            }   
        }
        
    }
}
