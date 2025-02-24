#![allow(unused)]
use rand::Rng;

pub(crate) fn generate_random_vector(dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rand::thread_rng().gen()).collect()
}

pub(crate) fn baseline_cos_functional(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }

    let (dot_product, norm_a, norm_b) = a
        .iter()
        .zip(b)
        .map(|(a, b)| (a * b, a * a, b * b))
        .fold((0.0, 0.0, 0.0), |acc, x| {
            (acc.0 + x.0, acc.1 + x.1, acc.2 + x.2)
        });

    Some(1.0 - (dot_product / (norm_a.sqrt() * norm_b.sqrt())))
}

pub(crate) fn baseline_l2sq_functional(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }

    Some(a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum())
}

pub(crate) fn baseline_cos_procedural(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }

    let mut dot_product = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;

    for i in 0..a.len() {
        dot_product += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    Some(1.0 - (dot_product / (norm_a.sqrt() * norm_b.sqrt())))
}

pub(crate) fn baseline_l2sq_procedural(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }

    let mut sum = 0.0;
    for i in 0..a.len() {
        let diff = a[i] - b[i];
        sum += diff * diff;
    }

    Some(sum)
}

pub(crate) fn baseline_cos_unrolled(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }

    let mut i = 0;
    let mut remainder = a.len() % 8;

    // We do this manual unrolling to allow the compiler to vectorize
    // the loop and avoid some branching even if we're not doing it explicitly.
    // This made a significant difference in benchmarking ~4x
    let mut acc1 = 0.0;
    let mut acc2 = 0.0;
    let mut acc3 = 0.0;
    let mut acc4 = 0.0;
    let mut acc5 = 0.0;
    let mut acc6 = 0.0;
    let mut acc7 = 0.0;
    let mut acc8 = 0.0;

    let mut norm_a_acc1 = 0.0;
    let mut norm_a_acc2 = 0.0;
    let mut norm_a_acc3 = 0.0;
    let mut norm_a_acc4 = 0.0;
    let mut norm_a_acc5 = 0.0;
    let mut norm_a_acc6 = 0.0;
    let mut norm_a_acc7 = 0.0;
    let mut norm_a_acc8 = 0.0;

    let mut norm_b_acc1 = 0.0;
    let mut norm_b_acc2 = 0.0;
    let mut norm_b_acc3 = 0.0;
    let mut norm_b_acc4 = 0.0;
    let mut norm_b_acc5 = 0.0;
    let mut norm_b_acc6 = 0.0;
    let mut norm_b_acc7 = 0.0;
    let mut norm_b_acc8 = 0.0;

    while i < (a.len() - remainder) {
        let a1 = unsafe { *a.get_unchecked(i) };
        let a2 = unsafe { *a.get_unchecked(i + 1) };
        let a3 = unsafe { *a.get_unchecked(i + 2) };
        let a4 = unsafe { *a.get_unchecked(i + 3) };
        let a5 = unsafe { *a.get_unchecked(i + 4) };
        let a6 = unsafe { *a.get_unchecked(i + 5) };
        let a7 = unsafe { *a.get_unchecked(i + 6) };
        let a8 = unsafe { *a.get_unchecked(i + 7) };

        let b1 = unsafe { *b.get_unchecked(i) };
        let b2 = unsafe { *b.get_unchecked(i + 1) };
        let b3 = unsafe { *b.get_unchecked(i + 2) };
        let b4 = unsafe { *b.get_unchecked(i + 3) };
        let b5 = unsafe { *b.get_unchecked(i + 4) };
        let b6 = unsafe { *b.get_unchecked(i + 5) };
        let b7 = unsafe { *b.get_unchecked(i + 6) };
        let b8 = unsafe { *b.get_unchecked(i + 7) };

        acc1 += (a1 * b1);
        acc2 += (a2 * b2);
        acc3 += (a3 * b3);
        acc4 += (a4 * b4);
        acc5 += (a5 * b5);
        acc6 += (a6 * b6);
        acc7 += (a7 * b7);
        acc8 += (a8 * b8);

        norm_a_acc1 += (a1 * a1);
        norm_a_acc2 += (a2 * a2);
        norm_a_acc3 += (a3 * a3);
        norm_a_acc4 += (a4 * a4);
        norm_a_acc5 += (a5 * a5);
        norm_a_acc6 += (a6 * a6);
        norm_a_acc7 += (a7 * a7);
        norm_a_acc8 += (a8 * a8);

        norm_b_acc1 += (b1 * b1);
        norm_b_acc2 += (b2 * b2);
        norm_b_acc3 += (b3 * b3);
        norm_b_acc4 += (b4 * b4);
        norm_b_acc5 += (b5 * b5);
        norm_b_acc6 += (b6 * b6);
        norm_b_acc7 += (b7 * b7);
        norm_b_acc8 += (b8 * b8);

        i += 8;
    }

    acc1 += acc2;
    acc3 += acc4;
    acc5 += acc6;
    acc7 += acc8;

    norm_a_acc1 += norm_a_acc2;
    norm_a_acc3 += norm_a_acc4;
    norm_a_acc5 += norm_a_acc6;
    norm_a_acc7 += norm_a_acc8;

    norm_b_acc1 += norm_b_acc2;
    norm_b_acc3 += norm_b_acc4;
    norm_b_acc5 += norm_b_acc6;
    norm_b_acc7 += norm_b_acc8;

    acc1 += acc3;
    acc5 += acc7;

    norm_a_acc1 += norm_a_acc3;
    norm_a_acc5 += norm_a_acc7;

    norm_b_acc1 += norm_b_acc3;
    norm_b_acc5 += norm_b_acc7;

    acc1 += acc5;
    norm_a_acc1 += norm_a_acc5;
    norm_b_acc1 += norm_b_acc5;

    while i < a.len() {
        let a = unsafe { *a.get_unchecked(i) };
        let b = unsafe { *b.get_unchecked(i) };

        acc1 += (a * b);
        norm_a_acc1 += (a * a);
        norm_b_acc1 += (b * b);
    }

    Some(1.0 - (acc1 / (norm_a_acc1 * norm_b_acc1).sqrt()))
}

pub(crate) fn baseline_l2sq_unrolled(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }
    let mut i = 0;
    let mut remainder = a.len() % 8;

    // We do this manual unrolling to allow the compiler to vectorize
    // the loop and avoid some branching even if we're not doing it explicitly.
    // This made a significant difference in benchmarking ~4x
    let mut acc1 = 0.0;
    let mut acc2 = 0.0;
    let mut acc3 = 0.0;
    let mut acc4 = 0.0;
    let mut acc5 = 0.0;
    let mut acc6 = 0.0;
    let mut acc7 = 0.0;
    let mut acc8 = 0.0;

    while i < (a.len() - remainder) {
        let a1 = unsafe { *a.get_unchecked(i) };
        let a2 = unsafe { *a.get_unchecked(i + 1) };
        let a3 = unsafe { *a.get_unchecked(i + 2) };
        let a4 = unsafe { *a.get_unchecked(i + 3) };
        let a5 = unsafe { *a.get_unchecked(i + 4) };
        let a6 = unsafe { *a.get_unchecked(i + 5) };
        let a7 = unsafe { *a.get_unchecked(i + 6) };
        let a8 = unsafe { *a.get_unchecked(i + 7) };

        let b1 = unsafe { *b.get_unchecked(i) };
        let b2 = unsafe { *b.get_unchecked(i + 1) };
        let b3 = unsafe { *b.get_unchecked(i + 2) };
        let b4 = unsafe { *b.get_unchecked(i + 3) };
        let b5 = unsafe { *b.get_unchecked(i + 4) };
        let b6 = unsafe { *b.get_unchecked(i + 5) };
        let b7 = unsafe { *b.get_unchecked(i + 6) };
        let b8 = unsafe { *b.get_unchecked(i + 7) };

        let diff1 = a1 - b1;
        let diff2 = a2 - b2;
        let diff3 = a3 - b3;
        let diff4 = a4 - b4;
        let diff5 = a5 - b5;
        let diff6 = a6 - b6;
        let diff7 = a7 - b7;
        let diff8 = a8 - b8;

        acc1 += (diff1 * diff1);
        acc2 += (diff2 * diff2);
        acc3 += (diff3 * diff3);
        acc4 += (diff4 * diff4);
        acc5 += (diff5 * diff5);
        acc6 += (diff6 * diff6);
        acc7 += (diff7 * diff7);
        acc8 += (diff8 * diff8);

        i += 8;
    }

    acc1 += acc2;
    acc3 += acc4;
    acc5 += acc6;
    acc7 += acc8;

    acc1 += acc3;
    acc5 += acc7;

    acc1 += acc5;

    while i < a.len() {
        let a = unsafe { *a.get_unchecked(i) };
        let b = unsafe { *b.get_unchecked(i) };
        let diff = a - b;
        acc1 += (diff * diff);
    }

    Some(acc1)
}
