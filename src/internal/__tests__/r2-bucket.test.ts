import { describe, test, expect } from "vitest";
import * as R2 from "../r2-bucket";

describe("R2 Error Classes", () => {
  describe("R2RateLimitError", () => {
    test("should create instance with required fields", () => {
      const error = new R2.R2RateLimitError({
        key: "test-key",
        operation: "put" as const,
      });

      expect(error.key).toBe("test-key");
      expect(error.operation).toBe("put");
      expect(error.message).toContain("rate limit");
      expect(error.message).toContain("test-key");
    });

    test("should include retry metadata in message", () => {
      const error = new R2.R2RateLimitError({
        key: "test-key",
        operation: "put" as const,
        retryAfter: 1000,
      });

      expect(error.message).toContain("Retry after 1000ms");
    });
  });

  describe("R2ConcurrencyError", () => {
    test("should create instance with reason", () => {
      const error = new R2.R2ConcurrencyError({
        operation: "get" as const,
        reason: "TooMuchConcurrency",
      });

      expect(error.operation).toBe("get");
      expect(error.reason).toBe("TooMuchConcurrency");
      expect(error.message).toContain("concurrency");
    });

    test("should include key in message when provided", () => {
      const error = new R2.R2ConcurrencyError({
        key: "test-key",
        operation: "get" as const,
        reason: "Too many requests",
      });

      expect(error.message).toContain("test-key");
    });
  });

  describe("R2ObjectTooLargeError", () => {
    test("should create instance with size", () => {
      const error = new R2.R2ObjectTooLargeError({
        key: "test-key",
        operation: "put" as const,
        sizeBytes: 5000000000000,
      });

      expect(error.sizeBytes).toBe(5000000000000);
      expect(error.message).toContain("5000000000000 bytes");
    });

    test("should include limit in message when provided", () => {
      const error = new R2.R2ObjectTooLargeError({
        key: "test-key",
        operation: "put" as const,
        limit: 5497558138880,
      });

      expect(error.limit).toBe(5497558138880);
      expect(error.message).toContain("5497558138880 bytes");
    });
  });

  describe("R2InvalidKeyError", () => {
    test("should create instance with reason", () => {
      const error = new R2.R2InvalidKeyError({
        key: "",
        operation: "put" as const,
        reason: "Key must not be empty",
      });

      expect(error.key).toBe("");
      expect(error.reason).toContain("empty");
      expect(error.message).toContain("Invalid R2 key");
    });
  });

  describe("R2MetadataError", () => {
    test("should create instance with size", () => {
      const error = new R2.R2MetadataError({
        key: "test-key",
        operation: "put" as const,
        reason: "Metadata exceeds 8192 byte limit",
        sizeBytes: 9000,
      });

      expect(error.sizeBytes).toBe(9000);
      expect(error.message).toContain("9000 bytes");
    });
  });

  describe("R2PreconditionFailedError", () => {
    test("should create instance with condition", () => {
      const error = new R2.R2PreconditionFailedError({
        key: "test-key",
        operation: "put" as const,
        condition: "etagMatches failed",
      });

      expect(error.condition).toBe("etagMatches failed");
      expect(error.message).toContain("precondition failed");
    });
  });

  describe("R2MultipartError", () => {
    test("should create instance with uploadId and partNumber", () => {
      const error = new R2.R2MultipartError({
        key: "test-key",
        operation: "uploadPart" as const,
        reason: "Invalid part number",
        uploadId: "upload-123",
        partNumber: 0,
      });

      expect(error.uploadId).toBe("upload-123");
      expect(error.partNumber).toBe(0);
      expect(error.message).toContain("upload-123");
      expect(error.message).toContain("part: 0");
    });
  });

  describe("R2BucketNotFoundError", () => {
    test("should create instance with bucket name", () => {
      const error = new R2.R2BucketNotFoundError({
        operation: "get" as const,
        bucketName: "my-bucket",
      });

      expect(error.bucketName).toBe("my-bucket");
      expect(error.message).toContain("my-bucket");
      expect(error.message).toContain("not found");
    });
  });

  describe("R2NotEnabledError", () => {
    test("should create instance with operation", () => {
      const error = new R2.R2NotEnabledError({
        operation: "get" as const,
      });

      expect(error.operation).toBe("get");
      expect(error.message).toContain("not enabled");
    });
  });

  describe("R2AuthorizationError", () => {
    test("should create instance with reason", () => {
      const error = new R2.R2AuthorizationError({
        operation: "put" as const,
        reason: "AccessDenied",
      });

      expect(error.reason).toBe("AccessDenied");
      expect(error.message).toContain("authorization failed");
    });
  });

  describe("R2NetworkError", () => {
    test("should create instance with cause", () => {
      const cause = new Error("Network timeout");
      const error = new R2.R2NetworkError({
        key: "test-key",
        operation: "get" as const,
        reason: "Network timeout",
        cause,
      });

      expect(error.key).toBe("test-key");
      expect(error.cause).toBe(cause);
      expect(error.message).toContain("network error");
    });
  });
});
