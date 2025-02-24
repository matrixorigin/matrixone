/**
 * @brief Computes the inner distance of two vectors (same as dot product).
 * @param {Float64Array|Float32Array} a - The first vector.
 * @param {Float64Array|Float32Array} b - The second vector.
 * @returns {number} The inner distance of vectors a and b.
 */
export function inner(a: Float64Array | Float32Array, b: Float64Array | Float32Array): number {
  if (a.length !== b.length) {
    throw new Error("Vectors must have the same length");
  }

  let result = 0;
  for (let i = 0; i < a.length; i++) {
    result += a[i] * b[i];
  }
  return result;
}

/**
 * @brief Computes the inner distance of two vectors (same as inner product).
 * @param {Float64Array|Float32Array} a - The first vector.
 * @param {Float64Array|Float32Array} b - The second vector.
 * @returns {number} The inner distance of vectors a and b.
 */
export function dot(a: Float64Array | Float32Array, b: Float64Array | Float32Array): number {
  return inner(a, b);
}

/**
 * @brief Computes the squared Euclidean distance between two vectors.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} a - The first vector.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} b - The second vector.
 * @returns {number} The squared Euclidean distance between vectors a and b.
 */
export function sqeuclidean(
  a: Float64Array | Float32Array | Int8Array | Uint8Array,
  b: Float64Array | Float32Array | Int8Array | Uint8Array
): number {
  if (a.length !== b.length) {
    throw new Error("Vectors must have the same length");
  }

  let result = 0;
  for (let i = 0; i < a.length; i++) {
    result += (a[i] - b[i]) * (a[i] - b[i]);
  }
  return result;
}

/**
 * @brief Computes the L2 Euclidean distance between two vectors.
 * @param {Float64Array|Float32Array|Int8Array | Uint8Array} a - The first vector.
 * @param {Float64Array|Float32Array|Int8Array | Uint8Array} b - The second vector.
 * @returns {number} The L2 euclidean distance between vectors a and b.
 */
export function euclidean(
  a: Float64Array | Float32Array | Int8Array | Uint8Array,
  b: Float64Array | Float32Array | Int8Array | Uint8Array
): number {
  if (a.length !== b.length) {
    throw new Error("Vectors must have the same length");
  }

  let result = 0;
  for (let i = 0; i < a.length; i++) {
    result += (a[i] - b[i]) * (a[i] - b[i]);
  }
  return Math.sqrt(result);
}


/**
 * @brief Computes the cosine distance between two vectors.
 * @param {Float64Array|Float32Array|Int8Array} a - The first vector.
 * @param {Float64Array|Float32Array|Int8Array} b - The second vector.
 * @returns {number} The cosine distance between vectors a and b.
 */
export function cosine(
  a: Float64Array | Float32Array | Int8Array,
  b: Float64Array | Float32Array | Int8Array
): number {
  if (a.length !== b.length) {
    throw new Error("Vectors must have the same length");
  }

  let dotProduct = 0;
  let magnitudeA = 0;
  let magnitudeB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    magnitudeA += a[i] * a[i];
    magnitudeB += b[i] * b[i];
  }

  magnitudeA = Math.sqrt(magnitudeA);
  magnitudeB = Math.sqrt(magnitudeB);

  if (magnitudeA === 0 || magnitudeB === 0) {
    console.warn(
      "Warning: One of the magnitudes is zero. Cosine similarity is undefined."
    );
    return 0;
  }

  return 1 - dotProduct / (magnitudeA * magnitudeB);
}

/**
 * @brief Computes the bitwise Hamming distance between two vectors.
 * @param {Uint8Array} a - The first vector.
 * @param {Uint8Array} b - The second vector.
 * @returns {number} The Hamming distance between vectors a and b.
 */
export const hamming = (a: Uint8Array, b: Uint8Array): number => {
  if (a.length !== b.length) {
    throw new Error("Arrays must be of the same length");
  }

  let distance = 0;

  for (let i = 0; i < a.length; i++) {
    let xor = a[i] ^ b[i]; // XOR operation to find differing bits

    // Count the number of set bits (differing bits)
    while (xor > 0) {
      distance += xor & 1;
      xor >>= 1;
    }
  }

  return distance;
};

/**
 * @brief Computes the bitwise Jaccard similarity coefficient between two vectors.
 * @param {Uint8Array} a - The first vector.
 * @param {Uint8Array} b - The second vector.
 * @returns {number} The Jaccard similarity coefficient between vectors a and b.
 */
export const jaccard = (a: Uint8Array, b: Uint8Array): number => {
  if (a.length !== b.length) {
    throw new Error("Arrays must be of the same length");
  }

  let intersection = 0;
  let union = 0;

  for (let i = 0; i < a.length; i++) {
    let ai = a[i];
    let bi = b[i];

    // Count the number of set bits in a AND b for intersection
    let and = ai & bi;
    while (and > 0) {
      intersection += and & 1;
      and >>= 1;
    }

    // Count the number of set bits in a OR b for union
    let or = ai | bi;
    while (or > 0) {
      union += or & 1;
      or >>= 1;
    }
  }

  if (union === 0) return 0; // Avoid division by zero

  return 1 - intersection / union;
};

/**
 * @brief Computes the Kullback-Leibler divergence between two probability distributions.
 * @param {Float64Array|Float32Array} a - The first vector.
 * @param {Float64Array|Float32Array} b - The second vector.
 * @returns {number} The Jaccard similarity coefficient between vectors a and b.
 */
export const kullbackleibler = (a: Float64Array | Float32Array, b: Float64Array | Float32Array): number => {
  if (a.length !== b.length) {
    throw new Error("Arrays must be of the same length");
  }

  let divergence = 0.0;
  for (let i = 0; i < a.length; i++) {
    if (a[i] < 0 || b[i] < 0) {
      throw new Error("Negative values are not allowed in probability distributions");
    }
    if (b[i] === 0) {
      throw new Error(
        "Division by zero encountered in KL divergence calculation"
      );
    }
    divergence += a[i] * Math.log(a[i] / b[i]);
  }

  return divergence;
};

/**
 * @brief Computes the Jensen-Shannon distance between two probability distributions.
 * @param {Float64Array|Float32Array} a - The first probability distribution.
 * @param {Float64Array|Float32Array} b - The second probability distribution.
 * @returns {number} The Jensen-Shannon distance between distributions a and b.
 */
export const jensenshannon = (a: Float64Array | Float32Array, b: Float64Array | Float32Array): number => {
  if (a.length !== b.length) {
    throw new Error("Arrays must be of the same length");
  }

  let divergence = 0;
  for (let i = 0; i < a.length; i++) {
    if (a[i] < 0 || b[i] < 0) {
      throw new Error("Negative values are not allowed in probability distributions");
    }
    const m = (a[i] + b[i]) / 2;
    if (m > 0) {
      if (a[i] > 0) divergence += a[i] * Math.log(a[i] / m);
      if (b[i] > 0) divergence += b[i] * Math.log(b[i] / m);
    }
  }

  divergence /= 2;
  return Math.sqrt(divergence);
};


export default {
  sqeuclidean,
  euclidean,
  cosine,
  inner,
  hamming,
  jaccard,
  kullbackleibler,
  jensenshannon,
};
