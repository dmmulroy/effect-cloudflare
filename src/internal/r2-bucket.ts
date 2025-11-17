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
 * **Status Code:** 413
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
 * @see https://developers.cloudflare.com/r2/api/workers/workers-api-reference/ - Range request documentation
 *
 * Thrown when a key violates validation rules or range request is invalid.
 *
 * **Status Codes:** 400, 414, 416
 *
 * **Invalid keys:**
 * - Empty string
 * - UTF-8 encoded length > 1024 bytes
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
 * @see https://developers.cloudflare.com/r2/platform/limits/ - Metadata limit of 8192 bytes
 *
 * Thrown when metadata exceeds the 8192 byte limit (combined httpMetadata and customMetadata).
 *
 * **Status Code:** 413
 *
 * **Limit:** 8192 bytes total for httpMetadata and customMetadata combined
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
 *   // Metadata that exceeds 8192 bytes
 *   const largeMetadata = {
 *     description: "x".repeat(9000)
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
 * @category models
 */
export type R2BucketError =
  | R2RateLimitError
  | R2ConcurrencyError
  | R2ObjectTooLargeError
  | R2InvalidKeyError
  | R2MetadataError
  | R2PreconditionFailedError
  | R2MultipartError
  | R2BucketNotFoundError
  | R2NotEnabledError
  | R2AuthorizationError
  | R2NetworkError;

/**
 * @since 1.0.0
 * @category models
 */
export interface R2Range {
  readonly offset?: number;
  readonly length?: number;
  readonly suffix?: number;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface R2Conditional {
  readonly etagMatches?: string;
  readonly etagDoesNotMatch?: string;
  readonly uploadedBefore?: Date;
  readonly uploadedAfter?: Date;
  readonly secondsGranularity?: boolean;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface R2HTTPMetadata {
  readonly contentType?: string;
  readonly contentLanguage?: string;
  readonly contentDisposition?: string;
  readonly contentEncoding?: string;
  readonly cacheControl?: string;
  readonly cacheExpiry?: Date;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface R2Checksums {
  readonly md5?: ArrayBuffer;
  readonly sha1?: ArrayBuffer;
  readonly sha256?: ArrayBuffer;
  readonly sha384?: ArrayBuffer;
  readonly sha512?: ArrayBuffer;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface R2Object {
  readonly key: string;
  readonly version: string;
  readonly size: number;
  readonly etag: string;
  readonly httpEtag: string;
  readonly checksums: R2Checksums;
  readonly uploaded: Date;
  readonly httpMetadata?: R2HTTPMetadata;
  readonly customMetadata?: Record<string, string>;
  readonly range?: R2Range;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface R2ObjectBody extends R2Object {
  readonly body: ReadableStream;
  readonly bodyUsed: boolean;
  readonly arrayBuffer: () => Promise<ArrayBuffer>;
  readonly text: () => Promise<string>;
  readonly json: <T>() => Promise<T>;
  readonly blob: () => Promise<Blob>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface R2Objects<Metadata = unknown> {
  readonly objects: ReadonlyArray<R2Object>;
  readonly truncated: boolean;
  readonly cursor?: string;
  readonly delimitedPrefixes: ReadonlyArray<string>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface R2UploadedPart {
  readonly partNumber: number;
  readonly etag: string;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface GetOptions {
  readonly onlyIf: Option.Option<R2Conditional>;
  readonly range: Option.Option<R2Range>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface PutOptions {
  readonly onlyIf: Option.Option<R2Conditional>;
  readonly httpMetadata: Option.Option<R2HTTPMetadata>;
  readonly customMetadata: Option.Option<Record<string, string>>;
  readonly md5: Option.Option<ArrayBuffer | string>;
  readonly sha1: Option.Option<ArrayBuffer | string>;
  readonly sha256: Option.Option<ArrayBuffer | string>;
  readonly sha384: Option.Option<ArrayBuffer | string>;
  readonly sha512: Option.Option<ArrayBuffer | string>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface ListOptions {
  readonly limit: Option.Option<number>;
  readonly prefix: Option.Option<string>;
  readonly cursor: Option.Option<string>;
  readonly delimiter: Option.Option<string>;
  readonly startAfter: Option.Option<string>;
  readonly include: Option.Option<
    ReadonlyArray<"httpMetadata" | "customMetadata">
  >;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface MultipartOptions {
  readonly httpMetadata: Option.Option<R2HTTPMetadata>;
  readonly customMetadata: Option.Option<Record<string, string>>;
}

/**
 * @internal
 * @category error mapping
 *
 * Helper to extract clean reason from error message
 */
const extractReason = (message: string): string => {
  return message;
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

  // Handle error codes first
  if (code === 10006 || message.includes("NoSuchBucket")) {
    return new R2BucketNotFoundError({
      operation,
    });
  }

  if (code === 10042 || message.includes("Please enable")) {
    return new R2NotEnabledError({
      operation,
    });
  }

  // Handle by status code
  switch (status) {
    case 429:
      // Rate limit exceeded
      return new R2RateLimitError({
        key: key ?? "",
        operation,
      });

    case 412:
      // Precondition failed
      return new R2PreconditionFailedError({
        key: key ?? "",
        operation,
        condition: extractReason(message),
      });

    case 416:
      // Range not satisfiable
      return new R2InvalidKeyError({
        key: key ?? "",
        operation,
        reason: "Invalid range request",
      });

    case 414:
      // Key too long
      return new R2InvalidKeyError({
        key: key ?? "",
        operation,
        reason: "Key exceeds 1024 byte limit",
      });

    case 413: {
      // Disambiguate 413 errors
      if (message.includes("metadata") || message.includes("Metadata")) {
        return new R2MetadataError({
          key: key ?? "",
          operation,
          reason: "Metadata exceeds 8192 byte limit",
        });
      }
      // Object too large
      return new R2ObjectTooLargeError({
        key: key ?? "",
        operation,
      });
    }

    case 401:
    case 403:
      // Authorization errors
      return new R2AuthorizationError({
        operation,
        reason: extractReason(message),
      });

    case 400: {
      // Disambiguate 400 errors
      if (
        message.includes("key") &&
        (message.includes("empty") || message.includes("invalid"))
      ) {
        return new R2InvalidKeyError({
          key: key ?? "",
          operation,
          reason: extractReason(message),
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
          reason: extractReason(message),
        });
      }

      // Other 400 errors
      return new R2NetworkError({
        key,
        operation,
        reason: extractReason(message),
        cause: error,
      });
    }

    default:
      // Check for concurrency errors
      if (
        message.includes("TooMuchConcurrency") ||
        message.includes("concurrency")
      ) {
        return new R2ConcurrencyError({
          key,
          operation,
          reason: extractReason(message),
        });
      }

      // Network errors, 5xx, timeouts, unknown status
      return new R2NetworkError({
        key,
        operation,
        reason: message,
        cause: error,
      });
  }
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
      options: GetOptions,
    ): Effect.Effect<Option.Option<R2ObjectBody | R2Object>, R2BucketError>;
  };

  readonly put: (
    key: string,
    value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob,
    options?: PutOptions,
  ) => Effect.Effect<Option.Option<R2Object>, R2BucketError>;

  readonly delete: (
    keys: string | ReadonlyArray<string>,
  ) => Effect.Effect<void, R2BucketError>;

  readonly list: <Metadata = unknown>(
    options?: ListOptions,
  ) => Effect.Effect<R2Objects<Metadata>, R2BucketError>;

  readonly createMultipartUpload: (
    key: string,
    options?: MultipartOptions,
  ) => Effect.Effect<R2MultipartUpload, R2BucketError>;

  readonly resumeMultipartUpload: (
    key: string,
    uploadId: string,
  ) => R2MultipartUpload;
}

/**
 * @internal
 * Helper to convert R2Conditional from Option to native format
 */
const convertConditional = (
  conditional: Option.Option<R2Conditional>,
): R2Conditional | undefined => {
  return Option.getOrUndefined(conditional);
};

/**
 * @internal
 * Helper to convert R2Range from Option to native format
 */
const convertRange = (
  range: Option.Option<R2Range>,
): { offset?: number; length?: number; suffix?: number } | undefined => {
  return Option.getOrUndefined(range);
};

/**
 * @internal
 * Helper to convert R2HTTPMetadata from Option to native format
 */
const convertHTTPMetadata = (
  metadata: Option.Option<R2HTTPMetadata>,
): R2HTTPMetadata | undefined => {
  return Option.getOrUndefined(metadata);
};

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

    get: ((...args: unknown[]) => {
      const [key, options] = args as [string, GetOptions | undefined];

      if (!options) {
        return Effect.tryPromise({
          try: async () => {
            const result = await bucket.get(key as string);
            return result === null
              ? Option.none()
              : Option.some(result as R2ObjectBody);
          },
          catch: (error) => mapError(error, "get", key as string),
        });
      }

      const nativeOptions: any = {};
      if (Option.isSome(options.onlyIf)) {
        nativeOptions.onlyIf = convertConditional(options.onlyIf);
      }
      if (Option.isSome(options.range)) {
        nativeOptions.range = convertRange(options.range);
      }

      return Effect.tryPromise({
        try: async () => {
          const result = await bucket.get(key as string, nativeOptions);
          return result === null
            ? Option.none()
            : Option.some(result as R2ObjectBody | R2Object);
        },
        catch: (error) => mapError(error, "get", key as string),
      });
    }) as R2Bucket["get"],

    put: (key, value, options) =>
      Effect.tryPromise({
        try: async () => {
          const nativeOptions: any = {};
          if (options) {
            if (Option.isSome(options.onlyIf)) {
              nativeOptions.onlyIf = convertConditional(options.onlyIf);
            }
            if (Option.isSome(options.httpMetadata)) {
              nativeOptions.httpMetadata = convertHTTPMetadata(
                options.httpMetadata,
              );
            }
            if (Option.isSome(options.customMetadata)) {
              nativeOptions.customMetadata = Option.getOrUndefined(
                options.customMetadata,
              );
            }
            if (Option.isSome(options.md5)) {
              nativeOptions.md5 = Option.getOrUndefined(options.md5);
            }
            if (Option.isSome(options.sha1)) {
              nativeOptions.sha1 = Option.getOrUndefined(options.sha1);
            }
            if (Option.isSome(options.sha256)) {
              nativeOptions.sha256 = Option.getOrUndefined(options.sha256);
            }
            if (Option.isSome(options.sha384)) {
              nativeOptions.sha384 = Option.getOrUndefined(options.sha384);
            }
            if (Option.isSome(options.sha512)) {
              nativeOptions.sha512 = Option.getOrUndefined(options.sha512);
            }
          }

          const result = await bucket.put(
            key,
            value,
            Object.keys(nativeOptions).length > 0 ? nativeOptions : undefined,
          );
          return result === null
            ? Option.none()
            : Option.some(result as R2Object);
        },
        catch: (error) => mapError(error, "put", key),
      }),

    delete: (keys) =>
      Effect.tryPromise({
        try: async () => {
          await bucket.delete(keys as any);
        },
        catch: (error) =>
          mapError(error, "delete", typeof keys === "string" ? keys : keys[0]),
      }),

    list: <Metadata = unknown>(options?: ListOptions) =>
      Effect.tryPromise({
        try: async () => {
          const nativeOptions: any = {};
          if (options) {
            if (Option.isSome(options.limit)) {
              nativeOptions.limit = Option.getOrUndefined(options.limit);
            }
            if (Option.isSome(options.prefix)) {
              nativeOptions.prefix = Option.getOrUndefined(options.prefix);
            }
            if (Option.isSome(options.cursor)) {
              nativeOptions.cursor = Option.getOrUndefined(options.cursor);
            }
            if (Option.isSome(options.delimiter)) {
              nativeOptions.delimiter = Option.getOrUndefined(
                options.delimiter,
              );
            }
            if (Option.isSome(options.startAfter)) {
              nativeOptions.startAfter = Option.getOrUndefined(
                options.startAfter,
              );
            }
            if (Option.isSome(options.include)) {
              nativeOptions.include = Option.getOrUndefined(options.include);
            }
          }

          const result = await bucket.list(nativeOptions);
          return {
            objects: result.objects as ReadonlyArray<R2Object>,
            truncated: result.truncated,
            // @ts-expect-error `cursor` is a conditional field
            cursor: result.cursor,
            delimitedPrefixes:
              result.delimitedPrefixes as ReadonlyArray<string>,
          };
        },
        catch: (error) => mapError(error, "list"),
      }),

    createMultipartUpload: (key, options) =>
      Effect.tryPromise({
        try: async () => {
          const nativeOptions: any = {};
          if (options) {
            if (Option.isSome(options.httpMetadata)) {
              nativeOptions.httpMetadata = convertHTTPMetadata(
                options.httpMetadata,
              );
            }
            if (Option.isSome(options.customMetadata)) {
              nativeOptions.customMetadata = Option.getOrUndefined(
                options.customMetadata,
              );
            }
          }

          const upload = await bucket.createMultipartUpload(
            key,
            Object.keys(nativeOptions).length > 0 ? nativeOptions : undefined,
          );
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
export const layer = (bucket: globalThis.R2Bucket): Layer.Layer<R2Bucket> =>
  Layer.succeed(R2Bucket, make(bucket));

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
