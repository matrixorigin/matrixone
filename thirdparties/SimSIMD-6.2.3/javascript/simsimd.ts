import build from "node-gyp-build";
import * as path from "node:path";
import { existsSync } from "node:fs";
import { getFileName, getRoot } from "bindings";
import * as fallback from "./fallback.js";

let compiled: any;

try {
  let builddir = getBuildDir(getDirName());
  compiled = build(builddir);
} catch (e) {
  compiled = fallback;
  console.warn(
    "It seems like your environment does't support the native simsimd module, so we are providing a JS fallback."
  );
}

/**
 * @brief Computes the squared Euclidean distance between two vectors.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} a - The first vector.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} b - The second vector.
 * @returns {number} The squared Euclidean distance between vectors a and b.
 */
export const sqeuclidean = (
  a: Float64Array | Float32Array | Int8Array | Uint8Array,
  b: Float64Array | Float32Array | Int8Array | Uint8Array
): number => {
  return compiled.sqeuclidean(a, b);
};

/**
 * @brief Computes the Euclidean distance between two vectors.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} a - The first vector.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} b - The second vector.
 * @returns {number} The Euclidean distance between vectors a and b.
 */
export const euclidean = (
  a: Float64Array | Float32Array | Int8Array | Uint8Array,
  b: Float64Array | Float32Array | Int8Array | Uint8Array
): number => {
  return compiled.euclidean(a, b);
};

/**
 * @brief Computes the cosine distance between two vectors.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} a - The first vector.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} b - The second vector.
 * @returns {number} The cosine distance between vectors a and b.
 */
export const cosine = (
  a: Float64Array | Float32Array | Int8Array | Uint8Array,
  b: Float64Array | Float32Array | Int8Array | Uint8Array
): number => {
  return compiled.cosine(a, b);
};

/**
 * @brief Computes the inner product of two vectors (same as dot product).
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} a - The first vector.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} b - The second vector.
 * @returns {number} The inner product of vectors a and b.
 */
export const inner = (
  a: Float64Array | Float32Array | Int8Array | Uint8Array,
  b: Float64Array | Float32Array | Int8Array | Uint8Array
): number => {
  return compiled.inner(a, b);
};

/**
 * @brief Computes the dot product of two vectors (same as inner product).
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} a - The first vector.
 * @param {Float64Array|Float32Array|Int8Array|Uint8Array} b - The second vector.
 * @returns {number} The dot product of vectors a and b.
 */
export const dot = (
  a: Float64Array | Float32Array | Int8Array | Uint8Array,
  b: Float64Array | Float32Array | Int8Array | Uint8Array
): number => {
  return compiled.dot(a, b);
};

/**
 * @brief Computes the bitwise Hamming distance between two vectors.
 * @param {Uint8Array} a - The first vector.
 * @param {Uint8Array} b - The second vector.
 * @returns {number} The Hamming distance between vectors a and b.
 */
export const hamming = (a: Uint8Array, b: Uint8Array): number => {
  return compiled.hamming(a, b);
};

/**
 * @brief Computes the bitwise Jaccard similarity coefficient between two vectors.
 * @param {Uint8Array} a - The first vector.
 * @param {Uint8Array} b - The second vector.
 * @returns {number} The Jaccard similarity coefficient between vectors a and b.
 */
export const jaccard = (a: Uint8Array, b: Uint8Array): number => {
  return compiled.jaccard(a, b);
};

/**
 * @brief Computes the Kullback-Leibler divergence between two vectors.
 * @param {Float64Array|Float32Array} a - The first vector.
 * @param {Float64Array|Float32Array} b - The second vector.
 * @returns {number} The Kullback-Leibler divergence between vectors a and b.
 */
export const kullbackleibler = (a: Float64Array | Float32Array, b: Float64Array | Float32Array): number => {
  return compiled.kullbackleibler(a, b);
};

/**
 * @brief Computes the Jensen-Shannon divergence between two vectors.
 * @param {Float64Array|Float32Array} a - The first vector.
 * @param {Float64Array|Float32Array} b - The second vector.
 * @returns {number} The Jensen-Shannon divergence between vectors a and b.
 */
export const jensenshannon = (a: Float64Array | Float32Array, b: Float64Array | Float32Array): number => {
  return compiled.jensenshannon(a, b);
};

/**
 * Quantizes a floating-point vector into a binary vector (1 for positive values, 0 for non-positive values) and packs the result into a Uint8Array, where each element represents 8 binary values from the original vector.
 * This function is useful for preparing data for bitwise distance or similarity computations, such as Hamming or Jaccard indices.
 * 
 * @param {Float32Array | Float64Array | Int8Array} vector The floating-point vector to be quantized and packed.
 * @returns {Uint8Array} A Uint8Array where each byte represents 8 binary quantized values from the input vector.
 */
export const toBinary = (vector: Float32Array | Float64Array | Int8Array): Uint8Array => {
  const byteLength = Math.ceil(vector.length / 8);
  const packedVector = new Uint8Array(byteLength);

  for (let i = 0; i < vector.length; i++) {
    if (vector[i] > 0) {
      const byteIndex = Math.floor(i / 8);
      const bitPosition = 7 - (i % 8);
      packedVector[byteIndex] |= (1 << bitPosition);
    }
  }

  return packedVector;
};
export default {
  dot,
  inner,
  sqeuclidean,
  euclidean,
  cosine,
  hamming,
  jaccard,
  kullbackleibler,
  jensenshannon,
  toBinary,
};

/**
 * @brief Finds the directory where the native build of the simsimd module is located.
 * @param {string} dir - The directory to start the search from.
 */
function getBuildDir(dir: string) {
  if (existsSync(path.join(dir, "build"))) return dir;
  if (existsSync(path.join(dir, "prebuilds"))) return dir;
  if (path.basename(dir) === ".next") {
    // special case for next.js on custom node (not vercel)
    const sideways = path.join(dir, "..", "node_modules", "simsimd");
    if (existsSync(sideways)) return getBuildDir(sideways);
  }
  if (dir === "/") throw new Error("Could not find native build for simsimd");
  return getBuildDir(path.join(dir, ".."));
}

function getDirName() {
  try {
    if (__dirname) return __dirname;
  } catch (e) { }
  return getRoot(getFileName());
}
