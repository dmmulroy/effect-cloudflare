/**
 * @since 1.0.0
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import { dual } from "effect/Function";

const R2ErrorCode = {
  INTERNAL_ERROR: 10001,
  NO_SUCH_OBJECT_KEY: 10007,
  ENTITY_TOO_LARGE: 100100,
  ENTITY_TOO_SMALL: 10011,
  METADATA_TOO_LARGE: 10012,
  INVALID_OBJECT_NAME: 10020,
  INVALID_MAX_KEYS: 10022,
  NO_SUCH_UPLOAD: 10024,
  INVALID_PART: 10025,
  INVALID_ARGUMENT: 10029,
  PRECONDITION_FAILED: 10031,
  BAD_DIGEST: 10037,
  INVALID_RANGE: 10039,
  BAD_UPLOAD: 10048,
} as const;

/**
 * @since 1.0.0
 * @category type id
 */
export const TypeId: unique symbol = Symbol.for(
  "@effect-cloudflare/R2BucketError",
);

/**
 * @since 1.0.0
 * @category type id
 */
export type TypeId = typeof TypeId;

/**
 * @since 1.0.0
 * @category refinements
 */
export const isR2BucketError = (u: unknown): u is R2BucketError =>
  Predicate.hasProperty(u, TypeId);

/**
 * @since 1.0.0
 * @category models
 */
export const R2Operation = Schema.Literal(
  "head",
  "get",
  "put",
  "delete",
  "list",
  "createMultipartUpload",
  "resumeMultipartUpload",
  "uploadPart",
  "completeMultipartUpload",
  "abortMultipartUpload",
);

/**
 * @since 1.0.0
 * @category models
 */
export type R2Operation = typeof R2Operation.Type;

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/r2/platform/limits/ - "1 per second" write limit per key
 * @see https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ - Workers API documentation
 *
 * Thrown when R2 operations exceed the rate limit of 1 write per second to the same key.
 *
 * **Status Code:** 429
 *
 * **Trigger:** Writing to the same key more than once per second
 *
 * **Retry Strategy:** Implement exponential backoff with minimum 1 second delay
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // This will throw R2RateLimitError if called >1/sec on same key
 *   yield* bucket.put("counter", "1")
 *   yield* bucket.put("counter", "2")  // Too fast!
 * }).pipe(
 *   Effect.catchTag("R2RateLimitError", (error) =>
 *     Effect.gen(function* () {
 *       console.log(`Rate limited on key: ${error.key}`)
 *       yield* Effect.sleep(1000)
 *       // Retry logic here
 *     })
 *   )
 * )
 * ```
 */
export class R2RateLimitError extends Schema.TaggedError<R2RateLimitError>(
  "@effect-cloudflare/R2BucketError/RateLimit",
)("R2RateLimitError", {
  key: Schema.String,
  operation: R2Operation,
  retryAfter: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const retryMsg = this.retryAfter
      ? ` Retry after ${this.retryAfter}ms.`
      : "";
    return `R2 rate limit exceeded for key "${this.key}" during ${this.operation}.${retryMsg}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/r2/platform/troubleshooting/ - 5XX error handling
 *
 * Thrown when bucket receives excessive concurrent requests, which can trigger
 * bucket-wide read and write locks.
 *
 * **Error:** TooMuchConcurrency
 *
 * **Trigger:** Excessive simultaneous operations on bucket or object
 *
 * **Retry Strategy:** Reduce request rate and retry with exponential backoff
 *
 * @example
 * ```typescript
 * import { Effect, Schedule } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *   yield* bucket.get("key")
 * }).pipe(
 *   Effect.catchTag("R2ConcurrencyError", (error) =>
 *     Effect.gen(function* () {
 *       console.log("Too much concurrency, backing off...")
 *       yield* Effect.sleep(1000)
 *     })
 *   ),
 *   Effect.retry(Schedule.exponential("100 millis", 2))
 * )
 * ```
 */
export class R2ConcurrencyError extends Schema.TaggedError<R2ConcurrencyError>(
  "@effect-cloudflare/R2BucketError/Concurrency",
)("R2ConcurrencyError", {
  key: Schema.optional(Schema.String),
  operation: R2Operation,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const keyMsg = this.key ? ` for key "${this.key}"` : "";
    return `R2 concurrency limit exceeded${keyMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/r2/platform/limits/ - "5 TiB per object" limit
 *
 * Thrown when attempting to store an object that exceeds size limits.
 *
 * **Status Code:** 400
 * **Error Code:** 100100
 *
 * **Limits:**
 * - Maximum object size: 5 TiB (4.995 TiB)
 * - Single-part upload: 4.995 GiB maximum
 * - Multipart upload: 4.995 TiB maximum
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // Create a value larger than 4.995 GiB for single-part upload
 *   const largeValue = new Uint8Array(5 * 1024 * 1024 * 1024)
 *
 *   // This will throw R2ObjectTooLargeError
 *   yield* bucket.put("large-key", largeValue)
 * }).pipe(
 *   Effect.catchTag("R2ObjectTooLargeError", (error) =>
 *     Effect.log(`Object too large: ${error.sizeBytes} bytes`)
 *   )
 * )
 * ```
 */
export class R2ObjectTooLargeError extends Schema.TaggedError<R2ObjectTooLargeError>(
  "@effect-cloudflare/R2BucketError/ObjectTooLarge",
)("R2ObjectTooLargeError", {
  key: Schema.String,
  operation: R2Operation,
  sizeBytes: Schema.optional(Schema.Number),
  limit: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const sizeMsg = this.sizeBytes ? ` (${this.sizeBytes} bytes)` : "";
    const limitMsg = this.limit ? ` Maximum size is ${this.limit} bytes.` : "";
    return `R2 object too large for key "${this.key}"${sizeMsg}.${limitMsg}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/r2/platform/limits/ - Key length limit of 1024 bytes
 *
 * Thrown when a key violates validation rules.
 *
 * **Status Code:** 400
 * **Error Code:** 10020
 *
 * **Invalid keys:**
 * - Empty string
 * - UTF-8 encoded length > 1024 bytes
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // These will throw R2InvalidKeyError:
 *   yield* bucket.put("", "value")                    // Empty key
 *   yield* bucket.put("x".repeat(1025), "value")      // Too long
 * }).pipe(
 *   Effect.catchTag("R2InvalidKeyError", (error) =>
 *     Effect.log(`Invalid key: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class R2InvalidKeyError extends Schema.TaggedError<R2InvalidKeyError>(
  "@effect-cloudflare/R2BucketError/InvalidKey",
)("R2InvalidKeyError", {
  key: Schema.String,
  operation: R2Operation,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `Invalid R2 key "${this.key}" during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/r2/platform/limits/ - Metadata limit of 8192 bytes total
 *
 * Thrown when metadata exceeds size limits.
 *
 * **Status Code:** 400
 * **Error Code:** 10012
 *
 * **Limits:**
 * - customMetadata: 2048 bytes
 * - Total metadata (httpMetadata + customMetadata): 8192 bytes
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 * import * as Option from "effect/Option"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // Metadata that exceeds 2048 bytes
 *   const largeMetadata = {
 *     description: "x".repeat(3000)
 *   }
 *
 *   // This will throw R2MetadataError
 *   yield* bucket.put("key", "value", {
 *     customMetadata: Option.some(largeMetadata)
 *   })
 * }).pipe(
 *   Effect.catchTag("R2MetadataError", (error) =>
 *     Effect.log(`Metadata too large: ${error.sizeBytes} bytes`)
 *   )
 * )
 * ```
 */
export class R2MetadataError extends Schema.TaggedError<R2MetadataError>(
  "@effect-cloudflare/R2BucketError/Metadata",
)("R2MetadataError", {
  key: Schema.String,
  operation: R2Operation,
  reason: Schema.String,
  sizeBytes: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const sizeMsg = this.sizeBytes ? ` (${this.sizeBytes} bytes)` : "";
    return `Invalid R2 metadata for key "${this.key}"${sizeMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ - Conditional operations documentation
 *
 * Represents a failed precondition check during conditional operations.
 *
 * **Status Code:** 412
 *
 * **Important:** This error is informational. When preconditions fail, put() and get()
 * return Option.none() or null, not throw this error. This error may be thrown for
 * other operation types.
 *
 * **Conditions:**
 * - etagMatches: Object's etag must match
 * - etagDoesNotMatch: Object's etag must not match
 * - uploadedBefore: Object must be uploaded before date
 * - uploadedAfter: Object must be uploaded after date
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 * import * as Option from "effect/Option"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // Conditional put returns Option.none() on precondition failure
 *   const result = yield* bucket.put("key", "value", {
 *     onlyIf: Option.some({ etagMatches: "wrong-etag" })
 *   })
 *
 *   if (Option.isNone(result)) {
 *     console.log("Precondition failed")
 *   }
 * })
 * ```
 */
export class R2PreconditionFailedError extends Schema.TaggedError<R2PreconditionFailedError>(
  "@effect-cloudflare/R2BucketError/PreconditionFailed",
)("R2PreconditionFailedError", {
  key: Schema.String,
  operation: R2Operation,
  condition: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `R2 precondition failed for key "${this.key}" during ${this.operation}: ${this.condition}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ - Multipart upload documentation
 * @see https://developers.cloudflare.com/r2/platform/limits/ - "Maximum 10,000 parts allowed"
 *
 * Thrown when multipart upload operations violate constraints.
 *
 * **Status Code:** 400
 *
 * **Constraints:**
 * - Maximum 10,000 parts per upload
 * - Valid part numbers: 1-10000
 * - Invalid uploadId
 * - Parts uploaded out of order (in some cases)
 *
 * **Common errors:**
 * - NoSuchUpload: uploadId doesn't exist
 * - InvalidPart: Invalid part number
 * - EntityTooLarge: Too many parts
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   const upload = yield* bucket.createMultipartUpload("large-file")
 *
 *   // This will throw R2MultipartError (invalid part number)
 *   yield* upload.uploadPart(0, "data")  // Part numbers start at 1
 *
 *   // This will throw R2MultipartError (too many parts)
 *   for (let i = 1; i <= 10001; i++) {
 *     yield* upload.uploadPart(i, `part-${i}`)
 *   }
 * }).pipe(
 *   Effect.catchTag("R2MultipartError", (error) =>
 *     Effect.log(`Multipart error: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class R2MultipartError extends Schema.TaggedError<R2MultipartError>(
  "@effect-cloudflare/R2BucketError/Multipart",
)("R2MultipartError", {
  key: Schema.optional(Schema.String),
  operation: R2Operation,
  reason: Schema.String,
  uploadId: Schema.optional(Schema.String),
  partNumber: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const keyMsg = this.key ? ` for key "${this.key}"` : "";
    const uploadMsg = this.uploadId ? ` (uploadId: ${this.uploadId})` : "";
    const partMsg =
      this.partNumber !== undefined ? ` (part: ${this.partNumber})` : "";
    return `R2 multipart upload error${keyMsg}${uploadMsg}${partMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown when attempting to access a bucket that doesn't exist.
 *
 * **Error Code:** 10006
 *
 * **Error:** NoSuchBucket
 *
 * **Trigger:** Bucket doesn't exist or was deleted
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // This will throw R2BucketNotFoundError if bucket doesn't exist
 *   yield* bucket.get("key")
 * }).pipe(
 *   Effect.catchTag("R2BucketNotFoundError", (error) =>
 *     Effect.log("Bucket not found")
 *   )
 * )
 * ```
 */
export class R2BucketNotFoundError extends Schema.TaggedError<R2BucketNotFoundError>(
  "@effect-cloudflare/R2BucketError/BucketNotFound",
)("R2BucketNotFoundError", {
  operation: R2Operation,
  bucketName: Schema.optional(Schema.String),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const nameMsg = this.bucketName ? ` "${this.bucketName}"` : "";
    return `R2 bucket${nameMsg} not found during ${this.operation}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown when R2 is not enabled on the account.
 *
 * **Error Code:** 10042
 *
 * **Message:** "Please enable through the Cloudflare Dashboard"
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *   yield* bucket.get("key")
 * }).pipe(
 *   Effect.catchTag("R2NotEnabledError", (error) =>
 *     Effect.log("R2 not enabled on account")
 *   )
 * )
 * ```
 */
export class R2NotEnabledError extends Schema.TaggedError<R2NotEnabledError>(
  "@effect-cloudflare/R2BucketError/NotEnabled",
)("R2NotEnabledError", {
  operation: R2Operation,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `R2 not enabled on account during ${this.operation}. Please enable through the Cloudflare Dashboard.`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown for authentication and authorization failures.
 *
 * **Status Codes:** 401, 403
 *
 * **Common errors:**
 * - InvalidAccessKeyId
 * - AccessDenied
 * - SignatureDoesNotMatch
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *   yield* bucket.put("key", "value")
 * }).pipe(
 *   Effect.catchTag("R2AuthorizationError", (error) =>
 *     Effect.log(`Authorization failed: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class R2AuthorizationError extends Schema.TaggedError<R2AuthorizationError>(
  "@effect-cloudflare/R2BucketError/Authorization",
)("R2AuthorizationError", {
  operation: R2Operation,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `R2 authorization failed during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/r2/platform/troubleshooting/ - General error handling
 *
 * Thrown for network-level errors, timeouts, or unexpected failures during R2 operations.
 * Also serves as a catch-all for unclassified errors.
 *
 * **Status Codes:** Variable (often 5xx, timeouts, or connection errors)
 *
 * **Common scenarios:**
 * - Network connectivity issues
 * - Service unavailability (503)
 * - Gateway timeouts (504)
 * - Internal server errors (500)
 * - Unexpected runtime errors
 *
 * @example
 * ```typescript
 * import { Effect, Schedule } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *   yield* bucket.get("key")
 * }).pipe(
 *   Effect.catchTag("R2NetworkError", (error) =>
 *     Effect.gen(function* () {
 *       console.log(`Network error: ${error.reason}`)
 *       // Retry with exponential backoff
 *     })
 *   ),
 *   Effect.retry(
 *     Schedule.exponential("100 millis", 2).pipe(
 *       Schedule.compose(Schedule.recurs(3))
 *     )
 *   )
 * )
 * ```
 */
export class R2NetworkError extends Schema.TaggedError<R2NetworkError>(
  "@effect-cloudflare/R2BucketError/Network",
)("R2NetworkError", {
  key: Schema.optional(Schema.String),
  operation: R2Operation,
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const keyMsg = this.key ? ` for key "${this.key}"` : "";
    return `R2 network error${keyMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown when attempting to upload a part that is too small in a multipart upload.
 *
 * **Status Code:** 400
 * **Error Code:** 10011
 *
 * **Constraints:**
 * - All parts except the last must be >= 5 MiB
 * - Last part can be smaller
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *   const upload = yield* bucket.createMultipartUpload("large-file")
 *
 *   // This will throw R2ObjectTooSmallError
 *   yield* upload.uploadPart(1, "small")  // < 5 MiB
 * }).pipe(
 *   Effect.catchTag("R2ObjectTooSmallError", (error) =>
 *     Effect.log("Part too small for multipart upload")
 *   )
 * )
 * ```
 */
export class R2ObjectTooSmallError extends Schema.TaggedError<R2ObjectTooSmallError>(
  "@effect-cloudflare/R2BucketError/ObjectTooSmall",
)("R2ObjectTooSmallError", {
  key: Schema.String,
  operation: R2Operation,
  sizeBytes: Schema.optional(Schema.Number),
  partNumber: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const sizeMsg = this.sizeBytes ? ` (${this.sizeBytes} bytes)` : "";
    const partMsg =
      this.partNumber !== undefined ? ` part ${this.partNumber}` : "";
    return `R2 object too small for key "${this.key}"${partMsg}${sizeMsg} during ${this.operation}. Parts must be >= 5 MiB except the last part.`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown when a checksum validation fails during upload.
 *
 * **Status Code:** 400
 * **Error Code:** 10037
 *
 * **Supported algorithms:**
 * - MD5
 * - SHA-1
 * - SHA-256
 * - SHA-384
 * - SHA-512
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 * import * as Option from "effect/Option"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // This will throw R2BadDigestError if checksum doesn't match
 *   yield* bucket.put("key", "value", {
 *     md5: Option.some("wrong-checksum")
 *   })
 * }).pipe(
 *   Effect.catchTag("R2BadDigestError", (error) =>
 *     Effect.log(`Checksum mismatch: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class R2BadDigestError extends Schema.TaggedError<R2BadDigestError>(
  "@effect-cloudflare/R2BucketError/BadDigest",
)("R2BadDigestError", {
  key: Schema.String,
  operation: R2Operation,
  reason: Schema.String,
  algorithm: Schema.optional(Schema.String),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const algoMsg = this.algorithm ? ` (${this.algorithm})` : "";
    return `R2 checksum validation failed${algoMsg} for key "${this.key}" during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown when list operation receives invalid limit parameter.
 *
 * **Status Code:** 400
 * **Error Code:** 10022
 *
 * **Constraint:** limit must be positive integer <= 1000
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 * import * as Option from "effect/Option"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // This will throw R2InvalidMaxKeysError
 *   yield* bucket.list({ limit: Option.some(10000) })
 * }).pipe(
 *   Effect.catchTag("R2InvalidMaxKeysError", (error) =>
 *     Effect.log("Invalid limit for list operation")
 *   )
 * )
 * ```
 */
export class R2InvalidMaxKeysError extends Schema.TaggedError<R2InvalidMaxKeysError>(
  "@effect-cloudflare/R2BucketError/InvalidMaxKeys",
)("R2InvalidMaxKeysError", {
  operation: R2Operation,
  limit: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const limitMsg = this.limit !== undefined ? ` (${this.limit})` : "";
    return `R2 invalid MaxKeys parameter${limitMsg} during ${this.operation}. MaxKeys must be positive integer <= 1000.`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown when invalid range request is made.
 *
 * **Status Code:** 416
 * **Error Code:** 10039
 *
 * **Invalid ranges:**
 * - Offset beyond object size
 * - Invalid suffix value
 * - Invalid length value
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 * import * as Option from "effect/Option"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *
 *   // This will throw R2InvalidRangeError if offset is beyond object size
 *   yield* bucket.get("key", {
 *     range: Option.some({ offset: 999999999 }),
 *     onlyIf: Option.none()
 *   })
 * }).pipe(
 *   Effect.catchTag("R2InvalidRangeError", (error) =>
 *     Effect.log("Invalid range request")
 *   )
 * )
 * ```
 */
export class R2InvalidRangeError extends Schema.TaggedError<R2InvalidRangeError>(
  "@effect-cloudflare/R2BucketError/InvalidRange",
)("R2InvalidRangeError", {
  key: Schema.String,
  operation: R2Operation,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `R2 invalid range for key "${this.key}" during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown when invalid arguments or metadata are provided.
 *
 * **Status Code:** 400
 * **Error Code:** 10029
 *
 * **Common scenarios:**
 * - Missing required metadata
 * - Invalid metadata format
 * - Invalid argument values
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *   yield* bucket.put("key", "value")
 * }).pipe(
 *   Effect.catchTag("R2InvalidArgumentError", (error) =>
 *     Effect.log(`Invalid argument: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class R2InvalidArgumentError extends Schema.TaggedError<R2InvalidArgumentError>(
  "@effect-cloudflare/R2BucketError/InvalidArgument",
)("R2InvalidArgumentError", {
  operation: R2Operation,
  reason: Schema.String,
  key: Schema.optional(Schema.String),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const keyMsg = this.key ? ` for key "${this.key}"` : "";
    return `R2 invalid argument${keyMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 *
 * Thrown for internal server errors during R2 operations.
 *
 * **Status Code:** 500
 * **Error Code:** 10001
 *
 * @example
 * ```typescript
 * import { Effect, Schedule } from "effect"
 * import * as R2 from "@effect-cloudflare/R2Bucket"
 *
 * const program = Effect.gen(function* () {
 *   const bucket = yield* R2.R2Bucket
 *   yield* bucket.get("key")
 * }).pipe(
 *   Effect.catchTag("R2InternalError", (error) =>
 *     Effect.log("Internal server error, retrying...")
 *   ),
 *   Effect.retry(Schedule.exponential("100 millis", 2))
 * )
 * ```
 */
export class R2InternalError extends Schema.TaggedError<R2InternalError>(
  "@effect-cloudflare/R2BucketError/Internal",
)("R2InternalError", {
  operation: R2Operation,
  reason: Schema.String,
  key: Schema.optional(Schema.String),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const keyMsg = this.key ? ` for key "${this.key}"` : "";
    return `R2 internal error${keyMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * Error thrown when attempting to read an R2ObjectBody that has already been consumed.
 *
 * @since 1.0.0
 * @category errors
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import { R2Bucket } from "@effect-cloudflare/r2-bucket"
 *
 * Effect.gen(function* () {
 *   const bucket = yield* R2Bucket
 *   const obj = yield* bucket.get("key").pipe(Effect.flatten)
 *
 *   // First consumption
 *   yield* obj.text
 *
 *   // This will throw R2BodyAlreadyUsedError
 *   yield* obj.arrayBuffer
 * }).pipe(
 *   Effect.catchTag("R2BodyAlreadyUsedError", (error) =>
 *     Effect.log("Body already consumed")
 *   )
 * )
 * ```
 */
export class R2BodyAlreadyUsedError extends Schema.TaggedError<R2BodyAlreadyUsedError>(
  "@effect-cloudflare/R2BucketError/BodyAlreadyUsed",
)("R2BodyAlreadyUsedError", {
  key: Schema.String,
  method: Schema.Literal("arrayBuffer", "text", "json", "blob"),
}) {
  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `R2 object body for key "${this.key}" has already been consumed. Cannot call ${this.method}() on a used body.`;
  }
}

/**
 * Error thrown when JSON parsing fails on R2ObjectBody.json().
 *
 * @since 1.0.0
 * @category errors
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import { R2Bucket } from "@effect-cloudflare/r2-bucket"
 *
 * Effect.gen(function* () {
 *   const bucket = yield* R2Bucket
 *   const obj = yield* bucket.get("non-json-file.txt").pipe(Effect.flatten)
 *
 *   // This will throw R2BodyJSONParseError
 *   yield* obj.json()
 * }).pipe(
 *   Effect.catchTag("R2BodyJSONParseError", (error) =>
 *     Effect.log(`Failed to parse JSON: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class R2BodyJSONParseError extends Schema.TaggedError<R2BodyJSONParseError>(
  "@effect-cloudflare/R2BucketError/BodyJSONParse",
)("R2BodyJSONParseError", {
  key: Schema.String,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `Failed to parse R2 object body for key "${this.key}" as JSON: ${this.reason}`;
  }
}

/**
 * Error thrown when body consumption fails for reasons other than already-used or JSON parsing.
 *
 * @since 1.0.0
 * @category errors
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import { R2Bucket } from "@effect-cloudflare/r2-bucket"
 *
 * Effect.gen(function* () {
 *   const bucket = yield* R2Bucket
 *   const obj = yield* bucket.get("key").pipe(Effect.flatten)
 *
 *   // Network error or stream corruption
 *   yield* obj.text
 * }).pipe(
 *   Effect.catchTag("R2BodyConsumptionError", (error) =>
 *     Effect.log(`Body read failed: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class R2BodyConsumptionError extends Schema.TaggedError<R2BodyConsumptionError>(
  "@effect-cloudflare/R2BucketError/BodyConsumption",
)("R2BodyConsumptionError", {
  key: Schema.String,
  method: Schema.Literal("arrayBuffer", "text", "json", "blob"),
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `Failed to consume R2 object body for key "${this.key}" with ${this.method}(): ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category models
 */
export type R2BucketError =
  | R2RateLimitError
  | R2ConcurrencyError
  | R2ObjectTooLargeError
  | R2ObjectTooSmallError
  | R2InvalidKeyError
  | R2InvalidRangeError
  | R2MetadataError
  | R2PreconditionFailedError
  | R2MultipartError
  | R2BucketNotFoundError
  | R2NotEnabledError
  | R2AuthorizationError
  | R2BadDigestError
  | R2InvalidMaxKeysError
  | R2InvalidArgumentError
  | R2InternalError
  | R2NetworkError
  | R2BodyAlreadyUsedError
  | R2BodyJSONParseError
  | R2BodyConsumptionError;

export interface R2Object extends globalThis.R2Object {
  "~raw": globalThis.R2Object;
}

/**
 * R2ObjectBody with Effect-based body consumption methods.
 * Access the raw Promise-based R2ObjectBody via R2ObjectBodyRawSymbol.
 *
 * @since 1.0.0
 * @category models
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import { R2Bucket, R2ObjectBodyRawSymbol } from "@effect-cloudflare/r2-bucket"
 *
 * Effect.gen(function* () {
 *   const bucket = yield* R2Bucket
 *   const obj = yield* bucket.get("data.json").pipe(Effect.flatten)
 *
 *   // Effect-based consumption
 *   const data = yield* obj.json<{ name: string }>()
 *
 *   // Access raw R2ObjectBody if needed
 *   const raw = obj[R2ObjectBodyRawSymbol]
 *   const textPromise = raw.text() // Returns Promise<string>
 * })
 * ```
 */
export interface R2ObjectBody extends R2Object {
  readonly body: ReadableStream;
  readonly bodyUsed: boolean;
  readonly arrayBuffer: Effect.Effect<
    ArrayBuffer,
    R2BodyAlreadyUsedError | R2BodyConsumptionError
  >;
  readonly text: Effect.Effect<
    string,
    R2BodyAlreadyUsedError | R2BodyConsumptionError
  >;
  readonly json: <T>() => Effect.Effect<
    T,
    R2BodyAlreadyUsedError | R2BodyJSONParseError | R2BodyConsumptionError
  >;
  readonly blob: Effect.Effect<
    Blob,
    R2BodyAlreadyUsedError | R2BodyConsumptionError
  >;
  readonly "~raw": globalThis.R2ObjectBody;
}

/**
 * Wraps native R2ObjectBody with Effect-based body consumption methods.
 *
 * @param raw - The native R2ObjectBody from Cloudflare Workers
 * @param key - The object key for error messages
 * @returns R2ObjectBody with Effect-based methods
 */
const wrapR2Object = (raw: globalThis.R2Object): R2Object => {
  return {
    checksums: raw.checksums,
    customMetadata: raw.customMetadata,
    etag: raw.etag,
    httpEtag: raw.httpEtag,
    httpMetadata: raw.httpMetadata,
    key: raw.key,
    range: raw.range,
    size: raw.size,
    ssecKeyMd5: raw.ssecKeyMd5,
    storageClass: raw.storageClass,
    uploaded: raw.uploaded,
    version: raw.version,
    writeHttpMetadata: raw.writeHttpMetadata.bind(raw),
    "~raw": raw,
  };
};
/**
 * Wraps native R2ObjectBody with Effect-based body consumption methods.
 *
 * @param raw - The native R2ObjectBody from Cloudflare Workers
 * @param key - The object key for error messages
 * @returns R2ObjectBody with Effect-based methods
 */
const wrapR2ObjectBody = (
  raw: globalThis.R2ObjectBody,
  key: string,
): R2ObjectBody => {
  const wrapMethod = <T>(
    method: "arrayBuffer" | "text" | "blob",
    fn: () => Promise<T>,
  ): Effect.Effect<T, R2BodyAlreadyUsedError | R2BodyConsumptionError> =>
    Effect.tryPromise({
      try: fn,
      catch: (error) => {
        const message = error instanceof Error ? error.message : String(error);

        // Check for TypeError indicating body already used
        if (
          error instanceof TypeError &&
          (message.includes("Body is unusable") ||
            message.includes("already been read") ||
            message.includes("disturbed") ||
            message.includes("locked"))
        ) {
          return new R2BodyAlreadyUsedError({ key, method });
        }

        // Generic consumption error
        return new R2BodyConsumptionError({
          key,
          method,
          reason: message,
        });
      },
    });

  const wrapJSON = <T>(
    fn: () => Promise<T>,
  ): Effect.Effect<
    T,
    R2BodyAlreadyUsedError | R2BodyJSONParseError | R2BodyConsumptionError
  > =>
    Effect.tryPromise({
      try: fn,
      catch: (error) => {
        const message = error instanceof Error ? error?.message : String(error);

        // Check for TypeError indicating body already used
        if (
          error instanceof TypeError &&
          (message.includes("Body is unusable") ||
            message.includes("already been read") ||
            message.includes("disturbed") ||
            message.includes("locked"))
        ) {
          return new R2BodyAlreadyUsedError({ key, method: "json" });
        }

        // Check for SyntaxError from JSON.parse
        if (error instanceof SyntaxError) {
          return new R2BodyJSONParseError({ key, reason: message });
        }

        // Generic consumption error
        return new R2BodyConsumptionError({
          key,
          method: "json",
          reason: message,
        });
      },
    });

  return Object.assign(wrapR2Object(raw), {
    body: raw.body,
    bodyUsed: raw.bodyUsed,
    arrayBuffer: wrapMethod("arrayBuffer", () => raw.arrayBuffer()),
    blob: wrapMethod("blob", () => raw.blob()),
    json: <T>() => wrapJSON<T>(() => raw.json<T>()),
    text: wrapMethod("text", () => raw.text()),
    "~raw": raw,
  });
};

/**
 * @since 1.0.0
 * @category error mapping
 *
 * Maps native R2 errors to typed error classes.
 *
 * @param error - The caught error from R2 operation
 * @param operation - The R2 operation that failed
 * @param key - Optional key involved in the operation
 * @returns Typed R2BucketError
 */
const mapError = (
  error: unknown,
  operation: R2Operation,
  key?: string,
): R2BucketError => {
  const errorObj = error as Error;
  const message = errorObj?.message ?? String(error);
  const status = (error as any)?.status;
  const code = (error as any)?.code;

  // Priority 1: Check v4code (R2 error codes)
  switch (code) {
    case R2ErrorCode.INTERNAL_ERROR:
      return new R2InternalError({
        operation,
        reason: message,
        key,
      });

    case R2ErrorCode.ENTITY_TOO_LARGE:
      return new R2ObjectTooLargeError({
        key: key ?? "",
        operation,
      });

    case R2ErrorCode.ENTITY_TOO_SMALL:
      return new R2ObjectTooSmallError({
        key: key ?? "",
        operation,
      });

    case R2ErrorCode.METADATA_TOO_LARGE:
      return new R2MetadataError({
        key: key ?? "",
        operation,
        reason: "Metadata exceeds size limit",
      });

    case R2ErrorCode.INVALID_OBJECT_NAME:
      return new R2InvalidKeyError({
        key: key ?? "",
        operation,
        reason: message,
      });

    case R2ErrorCode.INVALID_MAX_KEYS:
      return new R2InvalidMaxKeysError({
        operation,
      });

    case R2ErrorCode.NO_SUCH_UPLOAD:
      return new R2MultipartError({
        key,
        operation,
        reason: "The specified multipart upload does not exist",
      });

    case R2ErrorCode.INVALID_PART:
      return new R2MultipartError({
        key,
        operation,
        reason: "One or more of the specified parts could not be found",
      });

    case R2ErrorCode.INVALID_ARGUMENT:
      return new R2InvalidArgumentError({
        operation,
        reason: message,
        key,
      });

    case R2ErrorCode.PRECONDITION_FAILED:
      return new R2PreconditionFailedError({
        key: key ?? "",
        operation,
        condition: message,
      });

    case R2ErrorCode.BAD_DIGEST:
      return new R2BadDigestError({
        key: key ?? "",
        operation,
        reason: message,
      });

    case R2ErrorCode.INVALID_RANGE:
      return new R2InvalidRangeError({
        key: key ?? "",
        operation,
        reason: message,
      });

    case R2ErrorCode.BAD_UPLOAD:
      return new R2MultipartError({
        key,
        operation,
        reason: "There was a problem with the multipart upload",
      });

    case 10006: // NoSuchBucket
      return new R2BucketNotFoundError({
        operation,
      });

    case 10042: // R2 not enabled
      return new R2NotEnabledError({
        operation,
      });
  }

  // Priority 2: Check HTTP status codes
  switch (status) {
    case 429:
      return new R2RateLimitError({
        key: key ?? "",
        operation,
      });

    case 412:
      return new R2PreconditionFailedError({
        key: key ?? "",
        operation,
        condition: message,
      });

    case 416:
      return new R2InvalidRangeError({
        key: key ?? "",
        operation,
        reason: message,
      });

    case 401:
    case 403:
      return new R2AuthorizationError({
        operation,
        reason: message,
      });

    case 500:
      return new R2InternalError({
        operation,
        reason: message,
        key,
      });

    case 400:
      // Fallback 400 handling via message inspection
      if (
        message.includes("key") &&
        (message.includes("empty") || message.includes("invalid"))
      ) {
        return new R2InvalidKeyError({
          key: key ?? "",
          operation,
          reason: message,
        });
      }

      if (message.includes("metadata") || message.includes("Metadata")) {
        return new R2MetadataError({
          key: key ?? "",
          operation,
          reason: message,
        });
      }

      if (
        message.includes("multipart") ||
        message.includes("part") ||
        message.includes("upload") ||
        message.includes("NoSuchUpload") ||
        message.includes("InvalidPart")
      ) {
        return new R2MultipartError({
          key,
          operation,
          reason: message,
        });
      }

      if (message.includes("digest") || message.includes("checksum")) {
        return new R2BadDigestError({
          key: key ?? "",
          operation,
          reason: message,
        });
      }

      if (message.includes("too large") || message.includes("exceeds")) {
        return new R2ObjectTooLargeError({
          key: key ?? "",
          operation,
        });
      }

      if (message.includes("too small")) {
        return new R2ObjectTooSmallError({
          key: key ?? "",
          operation,
        });
      }

      // Generic 400 error
      return new R2InvalidArgumentError({
        operation,
        reason: message,
        key,
      });
  }

  // Priority 3: Message-based detection for non-standard errors
  if (message.includes("NoSuchBucket")) {
    return new R2BucketNotFoundError({
      operation,
    });
  }

  if (message.includes("Please enable") || message.includes("not entitled")) {
    return new R2NotEnabledError({
      operation,
    });
  }

  if (
    message.includes("TooMuchConcurrency") ||
    message.includes("concurrency")
  ) {
    return new R2ConcurrencyError({
      key,
      operation,
      reason: message,
    });
  }

  // Fallback: Network/unknown error
  return new R2NetworkError({
    key,
    operation,
    reason: message,
    cause: error,
  });
};

/**
 * @since 1.0.0
 * @category models
 */
export interface R2MultipartUpload {
  readonly key: string;
  readonly uploadId: string;
  readonly uploadPart: (
    partNumber: number,
    value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob,
  ) => Effect.Effect<R2UploadedPart, R2BucketError>;
  readonly complete: (
    uploadedParts: ReadonlyArray<R2UploadedPart>,
  ) => Effect.Effect<R2Object, R2BucketError>;
  readonly abort: () => Effect.Effect<void, R2BucketError>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface R2Bucket {
  readonly head: (
    key: string,
  ) => Effect.Effect<Option.Option<R2Object>, R2BucketError>;

  readonly get: {
    (key: string): Effect.Effect<Option.Option<R2ObjectBody>, R2BucketError>;
    (
      key: string,
      options: R2GetOptions,
    ): Effect.Effect<Option.Option<R2ObjectBody | R2Object>, R2BucketError>;
  };

  readonly put: (
    key: string,
    value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob,
    options?: R2PutOptions,
  ) => Effect.Effect<Option.Option<R2Object>, R2BucketError>;

  readonly delete: (
    keys: string | ReadonlyArray<string>,
  ) => Effect.Effect<void, R2BucketError>;

  readonly list: (
    options?: R2ListOptions,
  ) => Effect.Effect<R2Objects, R2BucketError>;

  readonly createMultipartUpload: (
    key: string,
    options?: R2MultipartOptions,
  ) => Effect.Effect<R2MultipartUpload, R2BucketError>;

  readonly resumeMultipartUpload: (
    key: string,
    uploadId: string,
  ) => R2MultipartUpload;
}

/**
 * @internal
 * Helper to wrap a native R2MultipartUpload with Effect-based methods
 */
const wrapMultipartUpload = (
  upload: globalThis.R2MultipartUpload,
): R2MultipartUpload => {
  return {
    key: upload.key,
    uploadId: upload.uploadId,
    uploadPart: (partNumber, value) =>
      Effect.tryPromise({
        try: async () => {
          const result = await upload.uploadPart(partNumber, value);
          return {
            partNumber: result.partNumber,
            etag: result.etag,
          };
        },
        catch: (error) => mapError(error, "uploadPart", upload.key),
      }),
    complete: (uploadedParts) =>
      Effect.tryPromise({
        try: async () => {
          const result = await upload.complete(uploadedParts as any);
          return result as R2Object;
        },
        catch: (error) =>
          mapError(error, "completeMultipartUpload", upload.key),
      }),
    abort: () =>
      Effect.tryPromise({
        try: async () => {
          await upload.abort();
        },
        catch: (error) => mapError(error, "abortMultipartUpload", upload.key),
      }),
  };
};

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = (bucket: globalThis.R2Bucket): R2Bucket => {
  return {
    head: (key) =>
      Effect.tryPromise({
        try: async () => {
          const result = await bucket.head(key);
          return result === null
            ? Option.none()
            : Option.some(result as R2Object);
        },
        catch: (error) => mapError(error, "head", key),
      }),

    get: (key, options?: R2GetOptions) => {
      return Effect.tryPromise({
        try: async () => {
          const result = await bucket.get(key, options);

          if (result === null) {
            return Option.none();
          }

          if (isR2ObjectBody(result)) {
            const wrapped: Option.Option<R2ObjectBody> = Option.some(
              wrapR2ObjectBody(result, key),
            );
            return wrapped;
          }

          const wrapped: Option.Option<R2Object> = Option.some(
            wrapR2Object(result),
          );

          return wrapped;
        },
        catch: (error) => mapError(error, "get", key),
      }) as any;
    },

    put: (key, value, options) =>
      Effect.tryPromise({
        try: async () => {
          const result = await bucket.put(key, value, options);
          return result === null
            ? Option.none()
            : Option.some(result as R2Object);
        },
        catch: (error) => mapError(error, "put", key),
      }),

    delete: (keys) =>
      Effect.tryPromise({
        try: async () => {
          return bucket.delete(keys as any);
        },
        catch: (error) =>
          mapError(error, "delete", typeof keys === "string" ? keys : keys[0]),
      }),

    list: (options) =>
      Effect.tryPromise({
        try: async () => {
          return bucket.list(options);
        },
        catch: (error) => mapError(error, "list"),
      }),

    createMultipartUpload: (key, options) =>
      Effect.tryPromise({
        try: async () => {
          const upload = await bucket.createMultipartUpload(key, options);
          return wrapMultipartUpload(upload);
        },
        catch: (error) => mapError(error, "createMultipartUpload", key),
      }),

    resumeMultipartUpload: (key, uploadId) => {
      const upload = bucket.resumeMultipartUpload(key, uploadId);
      return wrapMultipartUpload(upload);
    },
  };
};

/**
 * @since 1.0.0
 * @category tags
 */
export const R2Bucket = Context.GenericTag<R2Bucket>(
  "@effect-cloudflare/R2Bucket",
);

/**
 * @since 1.0.0
 * @category layers
 */
export const layer = (bucket: globalThis.R2Bucket): Layer.Layer<R2Bucket> => {
  return Layer.succeed(R2Bucket, make(bucket));
};

/**
 * @since 1.0.0
 * @category combinators
 */
export const withR2Bucket: {
  (
    bucket: globalThis.R2Bucket,
  ): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    bucket: globalThis.R2Bucket,
  ): Effect.Effect<A, E, R>;
} = dual(
  2,
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    bucket: globalThis.R2Bucket,
  ): Effect.Effect<A, E, R> =>
    Effect.provideService(effect, R2Bucket, make(bucket)),
);

/**
 * @since 1.0.0
 * @internal
 */
function isR2ObjectBody(value: unknown): value is R2ObjectBody {
  const isNotNullable = Predicate.isNotNullable(value);
  const hasBody = Predicate.hasProperty(value, "body");
  const hasBodyUsed = Predicate.hasProperty(value, "bodyUsed");
  const hasArrayBuffer = Predicate.hasProperty(value, "arrayBuffer");
  const hasBytes = Predicate.hasProperty(value, "bytes");
  const hasText = Predicate.hasProperty(value, "text");
  const hasJson = Predicate.hasProperty(value, "json");
  const hasBlob = Predicate.hasProperty(value, "blob");

  return (
    isNotNullable &&
    hasBody &&
    hasBodyUsed &&
    hasArrayBuffer &&
    hasBytes &&
    hasText &&
    hasJson &&
    hasBlob
  );
}
