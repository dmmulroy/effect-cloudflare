import { describe, test, expect } from "vitest";
import * as KV from "../kv-namespace";

// Note: This test file tests the error classes and their construction.
// Testing mapError requires either exporting it or integration tests with actual KV operations.

describe("KV Error Classes", () => {
  describe("KVRateLimitError", () => {
    test("should create instance with required fields", () => {
      const error = new KV.KVRateLimitError({
        key: "test-key",
        operation: "put" as const,
      });

      expect(error.key).toBe("test-key");
      expect(error.operation).toBe("put");
      expect(error.message).toContain("rate limit");
      expect(error.message).toContain("test-key");
    });

    test("should include retry metadata in message", () => {
      const error = new KV.KVRateLimitError({
        key: "test-key",
        operation: "put" as const,
        retryAfter: 1000,
      });

      expect(error.message).toContain("Retry after 1000ms");
    });
  });

  describe("KVResponseTooLargeError", () => {
    test("should create instance with size", () => {
      const error = new KV.KVResponseTooLargeError({
        key: "test-key",
        operation: "get" as const,
        sizeBytes: 26214401,
      });

      expect(error.sizeBytes).toBe(26214401);
      expect(error.message).toContain("26214401 bytes");
    });
  });

  describe("KVJsonParseError", () => {
    test("should create instance with cause", () => {
      const cause = new Error("Invalid JSON");
      const error = new KV.KVJsonParseError({
        key: "test-key",
        operation: "get" as const,
        cause,
      });

      expect(error.key).toBe("test-key");
      expect(error.cause).toBe(cause);
      expect(error.message).toContain("parse JSON");
    });
  });

  describe("KVInvalidKeyError", () => {
    test("should create instance with reason", () => {
      const error = new KV.KVInvalidKeyError({
        key: "",
        operation: "put" as const,
        reason: "Key names must not be empty",
      });

      expect(error.key).toBe("");
      expect(error.reason).toContain("empty");
      expect(error.message).toContain("Invalid KV key");
    });
  });

  describe("KVInvalidValueError", () => {
    test("should create instance with size", () => {
      const error = new KV.KVInvalidValueError({
        key: "test-key",
        operation: "put" as const,
        reason: "Value exceeds 25 MiB limit",
        sizeBytes: 26214401,
      });

      expect(error.sizeBytes).toBe(26214401);
      expect(error.message).toContain("26214401 bytes");
    });
  });

  describe("KVMetadataError", () => {
    test("should create instance with size", () => {
      const error = new KV.KVMetadataError({
        key: "test-key",
        operation: "put" as const,
        reason: "Metadata exceeds 1024 byte limit",
        sizeBytes: 1025,
      });

      expect(error.sizeBytes).toBe(1025);
      expect(error.message).toContain("1025 bytes");
    });
  });

  describe("KVExpirationError", () => {
    test("should create instance with expiration value", () => {
      const error = new KV.KVExpirationError({
        key: "test-key",
        operation: "put" as const,
        reason: "Expiration TTL must be at least 60",
        expirationValue: 30,
      });

      expect(error.expirationValue).toBe(30);
      expect(error.message).toContain("(30)");
    });
  });

  describe("KVCacheTtlError", () => {
    test("should create instance with cacheTtl value", () => {
      const error = new KV.KVCacheTtlError({
        key: "test-key",
        operation: "get" as const,
        reason: "Cache TTL must be at least 60",
        cacheTtlValue: 30,
      });

      expect(error.cacheTtlValue).toBe(30);
      expect(error.message).toContain("(30)");
      expect(error.message).toContain("cacheTtl");
    });
  });

  describe("KVBulkLimitError", () => {
    test("should create instance with requested count", () => {
      const error = new KV.KVBulkLimitError({
        operation: "get" as const,
        reason: "You can request a maximum of 100 keys",
        requestedCount: 101,
      });

      expect(error.requestedCount).toBe(101);
      expect(error.message).toContain("(101 keys)");
      expect(error.message).toContain("bulk operation");
    });
  });

  describe("KVListLimitError", () => {
    test("should create instance with limit value", () => {
      const error = new KV.KVListLimitError({
        operation: "list" as const,
        reason: "Please specify an integer less than 1000",
        limitValue: 1001,
      });

      expect(error.limitValue).toBe(1001);
      expect(error.message).toContain("(1001)");
      expect(error.message).toContain("list operation");
    });
  });

  describe("KVNetworkError", () => {
    test("should create instance with optional key", () => {
      const error = new KV.KVNetworkError({
        operation: "get" as const,
        reason: "Service Unavailable",
      });

      expect(error.key).toBeUndefined();
      expect(error.reason).toBe("Service Unavailable");
      expect(error.message).toContain("network error");
    });

    test("should include key in message when provided", () => {
      const error = new KV.KVNetworkError({
        key: "test-key",
        operation: "get" as const,
        reason: "Timeout",
      });

      expect(error.key).toBe("test-key");
      expect(error.message).toContain("test-key");
    });
  });

  describe("Error type guard", () => {
    test("should identify KV namespace errors", () => {
      const error = new KV.KVRateLimitError({
        key: "test",
        operation: "put" as const,
      });

      expect(KV.isKVNamespaceError(error)).toBe(true);
      expect(KV.isKVNamespaceError(new Error("regular error"))).toBe(false);
      expect(KV.isKVNamespaceError(null)).toBe(false);
    });
  });
});

// Integration test examples (require actual KV namespace instance)
describe("KV Error Mapping Integration", () => {
  test.skip("should map 429 errors to KVRateLimitError", () => {
    // This would require a mock KV namespace that throws 429 errors
    // Implementation depends on whether mapError is exported
  });

  test.skip("should map 414 errors to KVInvalidKeyError", () => {
    // Mock test for key too long
  });

  test.skip("should map 413 value size errors to KVInvalidValueError", () => {
    // Mock test for value too large
  });

  test.skip("should map 413 metadata errors to KVMetadataError", () => {
    // Mock test for metadata too large
  });

  test.skip("should map 400 expiration errors to KVExpirationError", () => {
    // Mock test for invalid expiration
  });

  test.skip("should map 400 cacheTtl errors to KVCacheTtlError", () => {
    // Mock test for invalid cacheTtl
  });

  test.skip("should map bulk limit errors to KVBulkLimitError", () => {
    // Mock test for too many keys in bulk get
  });

  test.skip("should map list limit errors to KVListLimitError", () => {
    // Mock test for list limit exceeded
  });
});
