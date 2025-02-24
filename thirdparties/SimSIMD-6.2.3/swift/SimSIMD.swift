import CSimSIMD

public protocol SimSIMD {
    static var dataType: simsimd_datatype_t { get }
    static var cosine: simsimd_metric_dense_punned_t { get }
    static var dotProduct: simsimd_metric_dense_punned_t { get }
    static var squaredEuclidean: simsimd_metric_dense_punned_t { get }
}

extension Int8: SimSIMD {
    public static let dataType = simsimd_datatype_i8_k
    public static let cosine = find(kind: simsimd_metric_cosine_k, dataType: dataType)
    public static let dotProduct = find(kind: simsimd_metric_dot_k, dataType: dataType)
    public static let squaredEuclidean = find(kind: simsimd_metric_sqeuclidean_k, dataType: dataType)
}

@available(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0, *)
extension Float16: SimSIMD {
    public static let dataType = simsimd_datatype_f16_k
    public static let cosine = find(kind: simsimd_metric_cosine_k, dataType: dataType)
    public static let dotProduct = find(kind: simsimd_metric_dot_k, dataType: dataType)
    public static let squaredEuclidean = find(kind: simsimd_metric_sqeuclidean_k, dataType: dataType)
}

extension Float32: SimSIMD {
    public static let dataType = simsimd_datatype_f32_k
    public static let cosine = find(kind: simsimd_metric_cosine_k, dataType: dataType)
    public static let dotProduct = find(kind: simsimd_metric_inner_k, dataType: dataType)
    public static let squaredEuclidean = find(kind: simsimd_metric_sqeuclidean_k, dataType: dataType)
}

extension Float64: SimSIMD {
    public static let dataType = simsimd_datatype_f64_k
    public static let cosine = find(kind: simsimd_metric_cosine_k, dataType: dataType)
    public static let dotProduct = find(kind: simsimd_metric_dot_k, dataType: dataType)
    public static let squaredEuclidean = find(kind: simsimd_metric_sqeuclidean_k, dataType: dataType)
}

extension SimSIMD {
    @inlinable @inline(__always)
    public static func cosine<A, B>(_ a: A, _ b: B) -> Double? where A: Sequence, B: Sequence, A.Element == Self, B.Element == Self {
        perform(cosine, a: a, b: b)
    }

    @inlinable @inline(__always)
    public static func dot<A, B>(_ a: A, _ b: B) -> Double? where A: Sequence, B: Sequence, A.Element == Self, B.Element == Self {
        perform(dotProduct, a: a, b: b)
    }

    @inlinable @inline(__always)
    public static func sqeuclidean<A, B>(_ a: A, _ b: B) -> Double? where A: Sequence, B: Sequence, A.Element == Self, B.Element == Self {
        perform(squaredEuclidean, a: a, b: b)
    }
}

extension RandomAccessCollection where Element: SimSIMD {
    @inlinable @inline(__always)
    public func cosine<B>(_ b: B) -> Double? where B: Sequence, B.Element == Element {
        Element.cosine(self, b)
    }

    @inlinable @inline(__always)
    public func dot<B>(_ b: B) -> Double? where B: Sequence, B.Element == Element {
        Element.dot(self, b)
    }

    @inlinable @inline(__always)
    public func sqeuclidean<B>(_ b: B) -> Double? where B: Sequence, B.Element == Element {
        Element.sqeuclidean(self, b)
    }
}

@inlinable @inline(__always)
func perform<A, B>(_ metric: simsimd_metric_dense_punned_t, a: A, b: B) -> Double? where A: Sequence, B: Sequence, A.Element == B.Element {
    var distance: simsimd_distance_t = 0
    let result = a.withContiguousStorageIfAvailable { a in
        b.withContiguousStorageIfAvailable { b in
            guard a.count > 0 && a.count == b.count else { return false }
            metric(a.baseAddress, b.baseAddress, .init(a.count), &distance)
            return true
        }
    }
    guard result == true else { return nil }
    return distance
}

public typealias Capabilities = simsimd_capability_t

extension simsimd_capability_t: OptionSet, CustomStringConvertible {
    public var description: String {
        var components: [String] = []
        if contains(.neon) { components.append(".neon") }
        if contains(.sve) { components.append(".sve") }
        if contains(.sve2) { components.append(".sve2") }
        if contains(.haswell) { components.append(".haswell") }
        if contains(.skylake) { components.append(".skylake") }
        if contains(.ice) { components.append(".ice") }
        if contains(.genoa) { components.append(".genoa") }
        if contains(.sapphire) { components.append(".sapphire") }
        if contains(.turin) { components.append(".turin") }
        if contains(.sierra) { components.append(".sierra") }
        return "[\(components.joined(separator: ", "))]"
    }

    public static let available = simsimd_capabilities()

    public static let any = simsimd_cap_any_k
    public static let neon = simsimd_cap_neon_k
    public static let sve = simsimd_cap_sve_k
    public static let sve2 = simsimd_cap_sve2_k
    public static let haswell = simsimd_cap_haswell_k
    public static let skylake = simsimd_cap_skylake_k
    public static let ice = simsimd_cap_ice_k
    public static let genoa = simsimd_cap_genoa_k
    public static let sapphire = simsimd_cap_sapphire_k
    public static let turin = simsimd_cap_turin_k
    public static let sierra = simsimd_cap_sierra_k
}

@inline(__always)
private func find(kind: simsimd_metric_kind_t, dataType: simsimd_datatype_t) -> simsimd_metric_dense_punned_t {
    var output: simsimd_metric_dense_punned_t?
    var used = simsimd_capability_t.any
    // Use `withUnsafeMutablePointer` to safely cast `output` to the required pointer type.
    withUnsafeMutablePointer(to: &output) { outputPtr in
        // Cast the pointer to `UnsafeMutablePointer<simsimd_kernel_punned_t?>`
        let castedPtr = outputPtr.withMemoryRebound(to: Optional<simsimd_kernel_punned_t>.self, capacity: 1) { $0 }
        simsimd_find_kernel_punned(kind, dataType, .available, .any, castedPtr, &used)
    }
    guard let output else { fatalError("Could not find function \(kind) for \(dataType)") }
    return output
}
