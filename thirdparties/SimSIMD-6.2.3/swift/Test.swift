import SimSIMD
import XCTest

class SimSIMDTests: XCTestCase {
    override class func setUp() {
        print("Capabilities: \(Capabilities.available)")
    }

    func testCosineInt8() throws {
        let a: [Int8] = [3, 97, 127]
        let b: [Int8] = [3, 97, 127]
        let result = try XCTUnwrap(a.cosine(b))
        XCTAssertEqual(result, 0.00012027938, accuracy: 0.01)
    }

    func testCosineFloat16() throws {
        let a: [Float16] = [1.0, 2.0, 3.0]
        let b: [Float16] = [1.0, 2.0, 3.0]
        let result = try XCTUnwrap(a.cosine(b))
        XCTAssertEqual(result, 0.004930496, accuracy: 0.01)
    }

    func testCosineFloat32() throws {
        let a: [Float32] = [1.0, 2.0, 3.0]
        let b: [Float32] = [1.0, 2.0, 3.0]
        let result = try XCTUnwrap(a.cosine(b))
        XCTAssertEqual(result, 0.004930496, accuracy: 0.01)
    }

    func testCosineFloat64() throws {
        let a: [Float64] = [1.0, 2.0, 3.0]
        let b: [Float64] = [1.0, 2.0, 3.0]
        let result = try XCTUnwrap(a.cosine(b))
        XCTAssertEqual(result, 0.004930496, accuracy: 0.01)
    }

    func testInnerInt8() throws {
        let a: [Int8] = [1, 2, 3]
        let b: [Int8] = [4, 5, 6]
        let result = try XCTUnwrap(a.dot(b))
        XCTAssertEqual(result, 32.0, accuracy: 0.01)
    }

    func testDotFloat16() throws {
        let a: [Float16] = [1.0, 2.0, 3.0]
        let b: [Float16] = [4.0, 5.0, 6.0]
        let result = try XCTUnwrap(a.dot(b))
        XCTAssertEqual(result, 32.0, accuracy: 0.01)
    }

    func testDotFloat32() throws {
        let a: [Float32] = [1.0, 2.0, 3.0]
        let b: [Float32] = [4.0, 5.0, 6.0]
        let result = try XCTUnwrap(a.dot(b))
        XCTAssertEqual(result, 32.0, accuracy: 0.01)
    }

    func testDotFloat64() throws {
        let a: [Float64] = [1.0, 2.0, 3.0]
        let b: [Float64] = [4.0, 5.0, 6.0]
        let result = try XCTUnwrap(a.dot(b))
        XCTAssertEqual(result, 32.0, accuracy: 0.01)
    }

    func testSqeuclideanInt8() throws {
        let a: [Int8] = [1, 2, 3]
        let b: [Int8] = [4, 5, 6]
        let result = try XCTUnwrap(a.sqeuclidean(b))
        XCTAssertEqual(result, 27.0, accuracy: 0.01)
    }

    func testSqeuclideanFloat16() throws {
        let a: [Float16] = [1.0, 2.0, 3.0]
        let b: [Float16] = [4.0, 5.0, 6.0]
        let result = try XCTUnwrap(a.sqeuclidean(b))
        XCTAssertEqual(result, 27.0, accuracy: 0.01)
    }

    func testSqeuclideanFloat32() throws {
        let a: [Float32] = [1.0, 2.0, 3.0]
        let b: [Float32] = [4.0, 5.0, 6.0]
        let result = try XCTUnwrap(a.sqeuclidean(b))
        XCTAssertEqual(result, 27.0, accuracy: 0.01)
    }

    func testSqeuclideanFloat64() throws {
        let a: [Float64] = [1.0, 2.0, 3.0]
        let b: [Float64] = [4.0, 5.0, 6.0]
        let result = try XCTUnwrap(a.sqeuclidean(b))
        XCTAssertEqual(result, 27.0, accuracy: 0.01)
    }
}
