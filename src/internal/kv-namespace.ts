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
  "@effect-cloudflare/KVNamespaceError",
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
export const isKVNamespaceError = (u: unknown): u is KVNamespaceError =>
  Predicate.hasProperty(u, TypeId);

/**
 * @since 1.0.0
 * @category models
 */
export const KVOperation = Schema.Literal(
  "get",
  "put",
  "delete",
  "list",
  "getWithMetadata",
);

/**
 * @since 1.0.0
 * @category models
 */
export type KVOperation = typeof KVOperation.Type;

/**
 * @internal
 * @category error mapping reference
 *
 * # Additional Error Scenarios
 *
 * These errors are thrown by the KV runtime but may not be correctly mapped by `mapError`:
 *
 * ## Bulk Operation Errors
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/namespace.worker.ts#L143-L150 - Max/min key validation
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L139-L178 - Bulk operation tests
 *
 * - Max keys: `"KV GET_BULK failed: 400 You can request a maximum of 100 keys"`
 * - Min keys: `"KV GET_BULK failed: 400 You must request a minimum of 1 key"`
 * - Invalid type: `"KV GET_BULK failed: 400 \"invalid\" is not a valid type. Use \"json\" or \"text\""`
 * - Size limit: `"KV GET_BULK failed: 413 Total size of request exceeds the limit of 25MB"`
 *
 * **Current mapping:** Falls through to `KVBulkLimitError` (400) or `KVResponseTooLargeError` (413)
 *
 * ## List Operation Errors
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/validator.worker.ts#L131-L152 - List validation
 *
 * - Invalid limit: `"KV LIST failed: 400 Invalid key_count_limit of {X}. Please specify an integer greater than 0."`
 * - Limit too high: `"KV LIST failed: 400 Invalid key_count_limit of {X}. Please specify an integer less than 1000."`
 * - Prefix too long: `"KV LIST failed: 414 UTF-8 encoded length of {X} exceeds key length limit of 512."`
 *
 * **Current mapping:** Falls through to `KVListLimitError` (400) or `KVInvalidKeyError` (414)
 *
 * ## Get Operation Errors
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L280-L294 - CacheTtl validation test
 *
 * - Invalid cacheTtl: `"KV GET failed: 400 Invalid cache_ttl of {X}. Cache TTL must be at least 60."`
 *
 * **Current mapping:** Falls through to `KVCacheTtlError` (400)
 *
 * ## Not Found Behavior (Not an Error)
 * @see https://developers.cloudflare.com/kv/api/read-key-value-pairs/ - "the promise will resolve with the literal value null"
 *
 * - Missing keys return `Option.none()`, not errors
 * - 404 only exists in internal HTTP layer, not exposed to users
 * - `get()`, `getWithMetadata()` return `Option.Option<T>` for this reason
 */

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/kv/api/write-key-value-pairs/ - Rate limiting documentation
 * @see https://developers.cloudflare.com/kv/platform/limits/ - "1 write per second" per key limit
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/namespace.worker.ts - Runtime implementation
 *
 * Thrown when KV operations exceed the rate limit of 1 write per second to the same key.
 *
 * **Actual error format from runtime:**
 * ```
 * Error: KV PUT failed: 429 Too Many Requests
 * ```
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
 * import * as KV from "@effect-cloudflare/KVNamespace"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // This will throw KVRateLimitError if called >1/sec on same key
 *   yield* kv.put("counter", "1")
 *   yield* kv.put("counter", "2")  // Too fast!
 * }).pipe(
 *   Effect.catchTag("KVRateLimitError", (error) =>
 *     Effect.gen(function* () {
 *       console.log(`Rate limited on key: ${error.key}`)
 *       yield* Effect.sleep(1000)  // Wait 1 second
 *       // Retry logic here
 *     })
 *   )
 * )
 * ```
 */
export class KVRateLimitError extends Schema.TaggedError<KVRateLimitError>(
  "@effect-cloudflare/KVNamespaceError/RateLimit",
)("KVRateLimitError", {
  key: Schema.String,
  operation: KVOperation,
  retryAfter: Schema.optional(Schema.Number),
  recommendedBackoff: Schema.optional(Schema.Number),
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
      : this.recommendedBackoff
        ? ` Recommended backoff: ${this.recommendedBackoff}ms.`
        : "";
    return `KV rate limit exceeded for key "${this.key}" during ${this.operation}.${retryMsg}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/kv/api/read-key-value-pairs/ - "responses above this size will fail with a 413 Error"
 * @see https://developers.cloudflare.com/kv/platform/limits/ - "25 MiB" value size limit
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/constants.ts#L7 - MAX_VALUE_SIZE constant
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L254-L267 - Bulk get size limit test
 *
 * Thrown when a KV response exceeds the 25 MB limit (single key or bulk operation total).
 *
 * **Actual error formats from runtime:**
 * - Single value: `Error: KV PUT failed: 413 Value length of {X} exceeds limit of {Y}.`
 * - Bulk get: `Error: KV GET_BULK failed: 413 Total size of request exceeds the limit of 25MB`
 *
 * **Status Code:** 413
 *
 * **Limits:**
 * - Single value: 25 MiB (26,214,400 bytes)
 * - Bulk operation total: 25 MB
 * - Test mode: 1 KiB
 *
 * **Note:** For PUT operations with oversized values, see `KVInvalidValueError`
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // Bulk get that exceeds 25MB total
 *   const keys = Array.from({ length: 100 }, (_, i) => `large-value-${i}`)
 *   yield* kv.get(keys)  // May throw KVResponseTooLargeError
 * }).pipe(
 *   Effect.catchTag("KVResponseTooLargeError", (error) =>
 *     Effect.log(`Response too large for ${error.operation}`)
 *   )
 * )
 * ```
 */
export class KVResponseTooLargeError extends Schema.TaggedError<KVResponseTooLargeError>(
  "@effect-cloudflare/KVNamespaceError/ResponseTooLarge",
)("KVResponseTooLargeError", {
  key: Schema.String,
  operation: KVOperation,
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
    return `KV response too large for key "${this.key}"${sizeMsg}. Maximum size is 25 MB.`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/namespace.worker.ts#L94-L105 - JSON parsing in runtime
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L193-L200 - Bulk get JSON parse test
 *
 * Thrown when attempting to parse a non-JSON value as JSON during a get operation with type "json".
 *
 * **Actual error format from runtime:**
 * ```
 * Error: KV GET failed: 400 At least one of the requested keys corresponds to a non-json value
 * Error: KV GET_BULK failed: 400 At least one of the requested keys corresponds to a non-json value
 * ```
 *
 * **Status Code:** 400
 *
 * **Trigger:** Calling `get(key, "json")` or `get(keys, "json")` on a key containing non-JSON text
 *
 * **Prevention:** Store JSON-serialized values or use type "text" for non-JSON data
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // Store plain text
 *   yield* kv.put("greeting", "Hello, World!")
 *
 *   // This will throw KVJsonParseError
 *   yield* kv.get("greeting", "json")
 * }).pipe(
 *   Effect.catchTag("KVJsonParseError", (error) =>
 *     Effect.gen(function* () {
 *       console.log(`Invalid JSON in key: ${error.key}`)
 *       // Fallback to text type
 *       const kv = yield* KV.tag
 *       return yield* kv.get(error.key, "text")
 *     })
 *   )
 * )
 * ```
 */
export class KVJsonParseError extends Schema.TaggedError<KVJsonParseError>(
  "@effect-cloudflare/KVNamespaceError/JsonParse",
)("KVJsonParseError", {
  key: Schema.String,
  operation: KVOperation,
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
    return `Failed to parse JSON for key "${this.key}" during ${this.operation}.`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/kv/api/write-key-value-pairs/ - Key restrictions
 * @see https://developers.cloudflare.com/kv/platform/limits/ - "512 bytes" key size limit
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/validator.worker.ts#L18-L39 - Key validation
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L110-L113 - Key length test
 *
 * Thrown when a KV key violates validation rules.
 *
 * **Actual error formats from runtime:**
 * - Empty key: `Error: KV {OP} failed: 400 Key names must not be empty`
 * - Illegal names: `Error: KV {OP} failed: 400 Illegal key name "{./.}". Please use a different name.`
 * - Too long: `Error: KV {OP} failed: 414 UTF-8 encoded length of {X} exceeds key length limit of 512.`
 * - URL decode: `Error: KV {OP} failed: 400 Could not URL-decode key name`
 *
 * **Status Codes:** 400, 414
 *
 * **Invalid keys:**
 * - Empty string (`""`)
 * - Exactly `"."` or `".."`
 * - UTF-8 encoded length > 512 bytes
 * - Invalid URL encoding (when urlencoded param used)
 *
 * **Note:** Multi-byte Unicode characters count as multiple bytes toward the 512 limit
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // These will all throw KVInvalidKeyError:
 *   yield* kv.put("", "value")              // Empty key
 *   yield* kv.put(".", "value")             // Reserved name
 *   yield* kv.put("..", "value")            // Reserved name
 *   yield* kv.put("x".repeat(513), "value") // Too long
 * }).pipe(
 *   Effect.catchTag("KVInvalidKeyError", (error) =>
 *     Effect.log(`Invalid key: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class KVInvalidKeyError extends Schema.TaggedError<KVInvalidKeyError>(
  "@effect-cloudflare/KVNamespaceError/InvalidKey",
)("KVInvalidKeyError", {
  key: Schema.String,
  operation: KVOperation,
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
    return `Invalid KV key "${this.key}" during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/kv/platform/limits/ - "25 MiB" value size limit
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/namespace.worker.ts#L239-L241 - Value size validation
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L393-L411 - Value size test
 *
 * Thrown when attempting to put a value that exceeds the 25 MiB limit.
 *
 * **Actual error format from runtime:**
 * ```
 * Error: KV PUT failed: 413 Value length of {X} exceeds limit of {Y}.
 * ```
 *
 * **Status Code:** 413
 *
 * **Limit:** 25 MiB (26,214,400 bytes) in production, 1 KiB in test mode
 *
 * **Note:** This is distinct from `KVResponseTooLargeError` which applies to read operations
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // Create a value larger than 25 MiB
 *   const largeValue = new Uint8Array(26 * 1024 * 1024)
 *
 *   // This will throw KVInvalidValueError
 *   yield* kv.put("large-key", largeValue)
 * }).pipe(
 *   Effect.catchTag("KVInvalidValueError", (error) =>
 *     Effect.log(`Value too large: ${error.sizeBytes} bytes`)
 *   )
 * )
 * ```
 */
export class KVInvalidValueError extends Schema.TaggedError<KVInvalidValueError>(
  "@effect-cloudflare/KVNamespaceError/InvalidValue",
)("KVInvalidValueError", {
  key: Schema.String,
  operation: KVOperation,
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
    return `Invalid KV value for key "${this.key}"${sizeMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/kv/platform/limits/ - "1024 bytes" metadata limit
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/validator.worker.ts#L106-L117 - Metadata validation
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L412-L426 - Metadata size test
 *
 * Thrown when metadata exceeds the 1024 byte limit (serialized JSON).
 *
 * **Actual error format from runtime:**
 * ```
 * Error: KV PUT failed: 413 Metadata length of {X} exceeds limit of 1024.
 * ```
 *
 * **Status Code:** 413
 *
 * **Limit:** 1024 bytes (serialized JSON, including quotes and structural characters)
 *
 * **Important:** The limit applies to the serialized JSON string, not the original object size.
 * Structural characters like braces, quotes, and colons count toward the limit.
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // Metadata that serializes to >1024 bytes
 *   const largeMetadata = {
 *     description: "x".repeat(1000),
 *     tags: ["a", "b", "c"]
 *   }
 *
 *   // This will throw KVMetadataError
 *   yield* kv.put("key", "value", {
 *     metadata: Option.some(largeMetadata)
 *   })
 * }).pipe(
 *   Effect.catchTag("KVMetadataError", (error) =>
 *     Effect.log(`Metadata too large: ${error.sizeBytes} bytes`)
 *   )
 * )
 * ```
 */
export class KVMetadataError extends Schema.TaggedError<KVMetadataError>(
  "@effect-cloudflare/KVNamespaceError/Metadata",
)("KVMetadataError", {
  key: Schema.String,
  operation: KVOperation,
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
    return `Invalid KV metadata for key "${this.key}"${sizeMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/kv/api/write-key-value-pairs/ - "Minimum expiration window is 60 seconds"
 * @see https://developers.cloudflare.com/kv/platform/limits/ - "60 seconds" minimum cacheTtl
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/validator.worker.ts#L73-L104 - Expiration validation
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L351-L391 - Expiration tests
 *
 * Thrown when expiration or expirationTtl values are invalid.
 *
 * **Actual error formats from runtime:**
 * - Invalid TTL: `Error: KV PUT failed: 400 Invalid expiration_ttl of {X}. Please specify integer greater than 0.`
 * - TTL too short: `Error: KV PUT failed: 400 Invalid expiration_ttl of {X}. Expiration TTL must be at least 60.`
 * - Invalid expiration: `Error: KV PUT failed: 400 Invalid expiration of {X}. Please specify integer greater than the current number of seconds since the UNIX epoch.`
 * - Expiration too soon: `Error: KV PUT failed: 400 Invalid expiration of {X}. Expiration times must be at least 60 seconds in the future.`
 *
 * **Status Code:** 400
 *
 * **Rules:**
 * - `expirationTtl`: Must be integer ≥ 60 seconds
 * - `expiration`: Must be UNIX timestamp > now + 60 seconds
 * - Both are optional, but if provided must be valid
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 * import * as Option from "effect/Option"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // These will throw KVExpirationError:
 *   yield* kv.put("key", "value", {
 *     expirationTtl: Option.some(30)  // Too short
 *   })
 *
 *   yield* kv.put("key", "value", {
 *     expiration: Option.some(Date.now() / 1000)  // Already passed
 *   })
 * }).pipe(
 *   Effect.catchTag("KVExpirationError", (error) =>
 *     Effect.log(`Invalid expiration: ${error.reason}`)
 *   )
 * )
 *
 * // Valid usage:
 * const validProgram = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // Expires in 5 minutes
 *   yield* kv.put("key", "value", {
 *     expirationTtl: Option.some(300)
 *   })
 *
 *   // Expires at specific time (must be >60s in future)
 *   const futureTime = Math.floor(Date.now() / 1000) + 120
 *   yield* kv.put("key", "value", {
 *     expiration: Option.some(futureTime)
 *   })
 * })
 * ```
 */
export class KVExpirationError extends Schema.TaggedError<KVExpirationError>(
  "@effect-cloudflare/KVNamespaceError/Expiration",
)("KVExpirationError", {
  key: Schema.String,
  operation: KVOperation,
  reason: Schema.String,
  expirationValue: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const expMsg =
      this.expirationValue !== undefined ? ` (${this.expirationValue})` : "";
    return `Invalid KV expiration for key "${this.key}"${expMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/kv/platform/limits/ - "60 seconds" minimum cacheTtl
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L280-L294 - CacheTtl validation test
 *
 * Thrown when cacheTtl value in get operations is invalid.
 *
 * **Actual error format from runtime:**
 * ```
 * Error: KV GET failed: 400 Invalid cache_ttl of {X}. Cache TTL must be at least 60.
 * ```
 *
 * **Status Code:** 400
 *
 * **Rule:** cacheTtl must be integer ≥ 60 seconds
 *
 * **Note:** cacheTtl is optional and rarely used in practice
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 * import * as Option from "effect/Option"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // This will throw KVCacheTtlError
 *   yield* kv.get("key", { type: undefined, cacheTtl: Option.some(30) })
 * }).pipe(
 *   Effect.catchTag("KVCacheTtlError", (error) =>
 *     Effect.log(`Invalid cacheTtl: ${error.reason}`)
 *   )
 * )
 *
 * // Valid usage:
 * const validProgram = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // Cache for 5 minutes
 *   yield* kv.get("key", { type: undefined, cacheTtl: Option.some(300) })
 * })
 * ```
 */
export class KVCacheTtlError extends Schema.TaggedError<KVCacheTtlError>(
  "@effect-cloudflare/KVNamespaceError/CacheTtl",
)("KVCacheTtlError", {
  key: Schema.String,
  operation: KVOperation,
  reason: Schema.String,
  cacheTtlValue: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const ttlMsg =
      this.cacheTtlValue !== undefined ? ` (${this.cacheTtlValue})` : "";
    return `Invalid KV cacheTtl for key "${this.key}"${ttlMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/namespace.worker.ts#L143-L150 - Max/min key validation
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/test/plugins/kv/index.spec.ts#L139-L178 - Bulk operation tests
 *
 * Thrown when bulk get operations violate key count limits.
 *
 * **Actual error formats from runtime:**
 * ```
 * Error: KV GET_BULK failed: 400 You can request a maximum of 100 keys
 * Error: KV GET_BULK failed: 400 You must request a minimum of 1 key
 * Error: KV GET_BULK failed: 400 "invalid" is not a valid type. Use "json" or "text"
 * ```
 *
 * **Status Code:** 400
 *
 * **Limits:**
 * - Maximum: 100 keys per bulk get
 * - Minimum: 1 key per bulk get
 * - Valid types: "json" or "text"
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // This will throw KVBulkLimitError
 *   const keys = Array.from({ length: 101 }, (_, i) => `key-${i}`)
 *   yield* kv.get(keys)  // Too many keys
 * }).pipe(
 *   Effect.catchTag("KVBulkLimitError", (error) =>
 *     Effect.log(`Bulk limit exceeded: ${error.reason}`)
 *   )
 * )
 *
 * // Valid usage:
 * const validProgram = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // Request up to 100 keys
 *   const keys = Array.from({ length: 100 }, (_, i) => `key-${i}`)
 *   yield* kv.get(keys)
 * })
 * ```
 */
export class KVBulkLimitError extends Schema.TaggedError<KVBulkLimitError>(
  "@effect-cloudflare/KVNamespaceError/BulkLimit",
)("KVBulkLimitError", {
  operation: KVOperation,
  reason: Schema.String,
  requestedCount: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const countMsg =
      this.requestedCount !== undefined ? ` (${this.requestedCount} keys)` : "";
    return `KV bulk operation limit exceeded${countMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/validator.worker.ts#L131-L152 - List validation
 *
 * Thrown when list operations violate limit or prefix constraints.
 *
 * **Actual error formats from runtime:**
 * ```
 * Error: KV LIST failed: 400 Invalid key_count_limit of {X}. Please specify an integer greater than 0.
 * Error: KV LIST failed: 400 Invalid key_count_limit of {X}. Please specify an integer less than 1000.
 * Error: KV LIST failed: 414 UTF-8 encoded length of {X} exceeds key length limit of 512.
 * ```
 *
 * **Status Codes:** 400, 414
 *
 * **Limits:**
 * - key_count_limit: 1 to 1000
 * - prefix: UTF-8 length ≤ 512 bytes
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 * import * as Option from "effect/Option"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // This will throw KVListLimitError
 *   yield* kv.list({ limit: Option.some(1001) })  // Limit too high
 * }).pipe(
 *   Effect.catchTag("KVListLimitError", (error) =>
 *     Effect.log(`List limit exceeded: ${error.reason}`)
 *   )
 * )
 *
 * // Valid usage:
 * const validProgram = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *
 *   // List up to 1000 keys
 *   yield* kv.list({ limit: Option.some(1000) })
 * })
 * ```
 */
export class KVListLimitError extends Schema.TaggedError<KVListLimitError>(
  "@effect-cloudflare/KVNamespaceError/ListLimit",
)("KVListLimitError", {
  operation: KVOperation,
  reason: Schema.String,
  limitValue: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const limitMsg =
      this.limitValue !== undefined ? ` (${this.limitValue})` : "";
    return `KV list operation limit exceeded${limitMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/workers/observability/errors/ - General Workers error handling
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/namespace.worker.ts - Runtime implementation
 *
 * Thrown for network-level errors, timeouts, or unexpected failures during KV operations.
 * Also serves as a catch-all for unclassified errors.
 *
 * **Status Code:** Variable (often 5xx, timeouts, or connection errors)
 *
 * **Common scenarios:**
 * - Network connectivity issues
 * - Service unavailability (503)
 * - Gateway timeouts (504)
 * - Internal server errors (500)
 * - Unexpected runtime errors
 * - Unclassified 400-level errors (cacheTtl, bulk limits, etc.)
 *
 * **Note:** Some validation errors currently fall into this category due to `mapError` limitations:
 * - Invalid cacheTtl (400)
 * - Bulk operation limits (400)
 * - List operation limits (400)
 *
 * @example
 * ```typescript
 * import { Effect, Schedule } from "effect"
 * import * as KV from "@effect-cloudflare/KVNamespace"
 *
 * const program = Effect.gen(function* () {
 *   const kv = yield* KV.tag
 *   yield* kv.get("key")
 * }).pipe(
 *   Effect.catchTag("KVNetworkError", (error) =>
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
export class KVNetworkError extends Schema.TaggedError<KVNetworkError>(
  "@effect-cloudflare/KVNamespaceError/Network",
)("KVNetworkError", {
  key: Schema.optional(Schema.String),
  operation: KVOperation,
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
    return `KV network error${keyMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category models
 */
export type KVNamespaceError =
  | KVRateLimitError
  | KVResponseTooLargeError
  | KVJsonParseError
  | KVInvalidKeyError
  | KVInvalidValueError
  | KVMetadataError
  | KVExpirationError
  | KVCacheTtlError
  | KVBulkLimitError
  | KVListLimitError
  | KVNetworkError;

/**
 * @since 1.0.0
 * @category models
 */
export interface ListKey<Metadata = unknown, Key extends string = string> {
  readonly name: Key;
  readonly expiration: Option.Option<number>;
  readonly metadata: Option.Option<Metadata>;
}

/**
 * @since 1.0.0
 * @category models
 */
export type ListResult<Metadata = unknown, Key extends string = string> =
  | {
      readonly listComplete: false;
      readonly keys: ReadonlyArray<ListKey<Metadata, Key>>;
      readonly cursor: string;
      readonly cacheStatus: Option.Option<string>;
    }
  | {
      readonly listComplete: true;
      readonly keys: ReadonlyArray<ListKey<Metadata, Key>>;
      readonly cacheStatus: Option.Option<string>;
    };

/**
 * @since 1.0.0
 * @category models
 */
export interface GetOptions<Type> {
  readonly type: Type;
  readonly cacheTtl: Option.Option<number>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface PutOptions {
  readonly expiration: Option.Option<number>;
  readonly expirationTtl: Option.Option<number>;
  readonly metadata: Option.Option<unknown>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface ListOptions {
  readonly prefix: Option.Option<string>;
  readonly limit: Option.Option<number>;
  readonly cursor: Option.Option<string>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface GetWithMetadataResult<Value, Metadata> {
  readonly value: Option.Option<Value>;
  readonly metadata: Option.Option<Metadata>;
  readonly cacheStatus: Option.Option<string>;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface KVNamespace<Key extends string = string> {
  readonly get: {
    (
      key: Key,
      options?: Partial<GetOptions<undefined>>,
    ): Effect.Effect<Option.Option<string>, KVNamespaceError>;
    (
      key: Key,
      type: "text",
    ): Effect.Effect<Option.Option<string>, KVNamespaceError>;
    <ExpectedValue = unknown>(
      key: Key,
      type: "json",
    ): Effect.Effect<Option.Option<ExpectedValue>, KVNamespaceError>;
    (
      key: Key,
      type: "arrayBuffer",
    ): Effect.Effect<Option.Option<ArrayBuffer>, KVNamespaceError>;
    (
      key: Key,
      type: "stream",
    ): Effect.Effect<Option.Option<ReadableStream>, KVNamespaceError>;
    (
      key: Key,
      options: GetOptions<"text">,
    ): Effect.Effect<Option.Option<string>, KVNamespaceError>;
    <ExpectedValue = unknown>(
      key: Key,
      options: GetOptions<"json">,
    ): Effect.Effect<Option.Option<ExpectedValue>, KVNamespaceError>;
    (
      key: Key,
      options: GetOptions<"arrayBuffer">,
    ): Effect.Effect<Option.Option<ArrayBuffer>, KVNamespaceError>;
    (
      key: Key,
      options: GetOptions<"stream">,
    ): Effect.Effect<Option.Option<ReadableStream>, KVNamespaceError>;
    (
      key: ReadonlyArray<Key>,
      type: "text",
    ): Effect.Effect<
      ReadonlyMap<string, Option.Option<string>>,
      KVNamespaceError
    >;
    <ExpectedValue = unknown>(
      key: ReadonlyArray<Key>,
      type: "json",
    ): Effect.Effect<
      ReadonlyMap<string, Option.Option<ExpectedValue>>,
      KVNamespaceError
    >;
    (
      key: ReadonlyArray<Key>,
      options?: Partial<GetOptions<undefined>>,
    ): Effect.Effect<
      ReadonlyMap<string, Option.Option<string>>,
      KVNamespaceError
    >;
    (
      key: ReadonlyArray<Key>,
      options: GetOptions<"text">,
    ): Effect.Effect<
      ReadonlyMap<string, Option.Option<string>>,
      KVNamespaceError
    >;
    <ExpectedValue = unknown>(
      key: ReadonlyArray<Key>,
      options: GetOptions<"json">,
    ): Effect.Effect<
      ReadonlyMap<string, Option.Option<ExpectedValue>>,
      KVNamespaceError
    >;
  };

  readonly list: <Metadata = unknown>(
    options?: ListOptions,
  ) => Effect.Effect<ListResult<Metadata, Key>, KVNamespaceError>;

  readonly put: (
    key: Key,
    value: string | ArrayBuffer | ArrayBufferView | ReadableStream,
    options?: PutOptions,
  ) => Effect.Effect<void, KVNamespaceError>;

  readonly getWithMetadata: {
    <Metadata = unknown>(
      key: Key,
      options?: Partial<GetOptions<undefined>>,
    ): Effect.Effect<GetWithMetadataResult<string, Metadata>, KVNamespaceError>;
    <Metadata = unknown>(
      key: Key,
      type: "text",
    ): Effect.Effect<GetWithMetadataResult<string, Metadata>, KVNamespaceError>;
    <ExpectedValue = unknown, Metadata = unknown>(
      key: Key,
      type: "json",
    ): Effect.Effect<
      GetWithMetadataResult<ExpectedValue, Metadata>,
      KVNamespaceError
    >;
    <Metadata = unknown>(
      key: Key,
      type: "arrayBuffer",
    ): Effect.Effect<
      GetWithMetadataResult<ArrayBuffer, Metadata>,
      KVNamespaceError
    >;
    <Metadata = unknown>(
      key: Key,
      type: "stream",
    ): Effect.Effect<
      GetWithMetadataResult<ReadableStream, Metadata>,
      KVNamespaceError
    >;
    <Metadata = unknown>(
      key: Key,
      options: GetOptions<"text">,
    ): Effect.Effect<GetWithMetadataResult<string, Metadata>, KVNamespaceError>;
    <ExpectedValue = unknown, Metadata = unknown>(
      key: Key,
      options: GetOptions<"json">,
    ): Effect.Effect<
      GetWithMetadataResult<ExpectedValue, Metadata>,
      KVNamespaceError
    >;
    <Metadata = unknown>(
      key: Key,
      options: GetOptions<"arrayBuffer">,
    ): Effect.Effect<
      GetWithMetadataResult<ArrayBuffer, Metadata>,
      KVNamespaceError
    >;
    <Metadata = unknown>(
      key: Key,
      options: GetOptions<"stream">,
    ): Effect.Effect<
      GetWithMetadataResult<ReadableStream, Metadata>,
      KVNamespaceError
    >;
    <Metadata = unknown>(
      key: ReadonlyArray<Key>,
      type: "text",
    ): Effect.Effect<
      ReadonlyMap<string, GetWithMetadataResult<string, Metadata>>,
      KVNamespaceError
    >;
    <ExpectedValue = unknown, Metadata = unknown>(
      key: ReadonlyArray<Key>,
      type: "json",
    ): Effect.Effect<
      ReadonlyMap<string, GetWithMetadataResult<ExpectedValue, Metadata>>,
      KVNamespaceError
    >;
    <Metadata = unknown>(
      key: ReadonlyArray<Key>,
      options?: Partial<GetOptions<undefined>>,
    ): Effect.Effect<
      ReadonlyMap<string, GetWithMetadataResult<string, Metadata>>,
      KVNamespaceError
    >;
    <Metadata = unknown>(
      key: ReadonlyArray<Key>,
      options: GetOptions<"text">,
    ): Effect.Effect<
      ReadonlyMap<string, GetWithMetadataResult<string, Metadata>>,
      KVNamespaceError
    >;
    <ExpectedValue = unknown, Metadata = unknown>(
      key: ReadonlyArray<Key>,
      options: GetOptions<"json">,
    ): Effect.Effect<
      ReadonlyMap<string, GetWithMetadataResult<ExpectedValue, Metadata>>,
      KVNamespaceError
    >;
  };

  readonly delete: (key: Key) => Effect.Effect<void, KVNamespaceError>;
}

/**
 * @internal
 * @category error mapping
 *
 * Helper to extract clean reason from error message
 * Removes "KV {OP} failed: {STATUS} " prefix
 */
const extractReason = (message: string): string => {
  const match = message.match(/KV \w+ failed: \d+ (.+)/);
  return match?.[1] ?? message;
};

/**
 * @since 1.0.0
 * @category error mapping
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/namespace.worker.ts - Miniflare HttpError implementation
 * @see https://github.com/cloudflare/workers-sdk/blob/main/packages/miniflare/src/workers/kv/validator.worker.ts - Validation error patterns
 *
 * Maps native KV errors to typed error classes.
 *
 * **Runtime Error Structure:**
 * ```typescript
 * class HttpError extends Error {
 *   status: number;  // 400, 413, 414, 429, etc.
 *   message: string; // "Value length of X exceeds limit of Y."
 * }
 * ```
 *
 * **Error Message Format:** `"KV {OPERATION} failed: {STATUS} {MESSAGE}"`
 *
 * @param error - The caught error from KV operation
 * @param operation - The KV operation that failed
 * @param key - Optional key involved in the operation
 * @returns Typed KVNamespaceError
 */
const mapError = (
  error: unknown,
  operation: KVOperation,
  key?: string,
): KVNamespaceError => {
  const errorObj = error as Error;
  const message = errorObj?.message ?? String(error);
  const status = (error as any)?.status;

  // Primary classification by HTTP status code
  switch (status) {
    case 429:
      // Rate limit: 1 write/sec/key exceeded
      return new KVRateLimitError({
        key: key ?? "",
        operation,
      });

    case 414:
      // Key too long (>512 bytes) or prefix too long in list
      if (message.includes("key_count_limit") || operation === "list") {
        return new KVListLimitError({
          operation,
          reason: extractReason(message),
        });
      }
      return new KVInvalidKeyError({
        key: key ?? "",
        operation,
        reason: "Key exceeds 512 byte limit",
      });

    case 413: {
      // Disambiguate 413 errors by message content
      if (message.includes("Value length")) {
        // PUT: Value exceeds 25 MiB
        const sizeMatch = message.match(/Value length of (\d+)/);
        return new KVInvalidValueError({
          key: key ?? "",
          operation,
          reason: "Value exceeds 25 MiB limit",
          sizeBytes: sizeMatch?.[1] ? parseInt(sizeMatch[1]) : undefined,
        });
      } else if (message.includes("Metadata length")) {
        // PUT: Metadata exceeds 1024 bytes
        const sizeMatch = message.match(/Metadata length of (\d+)/);
        return new KVMetadataError({
          key: key ?? "",
          operation,
          reason: "Metadata exceeds 1024 byte limit",
          sizeBytes: sizeMatch?.[1] ? parseInt(sizeMatch[1]) : undefined,
        });
      } else if (message.includes("Total size")) {
        // GET_BULK: Response exceeds 25 MB
        return new KVResponseTooLargeError({
          key: key ?? "",
          operation,
        });
      }
      // Fallback for other 413 errors
      return new KVResponseTooLargeError({
        key: key ?? "",
        operation,
      });
    }

    case 400: {
      // Disambiguate 400 errors by message content
      if (
        message.includes("Key names must not be empty") ||
        message.includes("Illegal key name") ||
        message.includes("Could not URL-decode")
      ) {
        // Invalid key name
        return new KVInvalidKeyError({
          key: key ?? "",
          operation,
          reason: extractReason(message),
        });
      } else if (
        message.includes("expiration") ||
        message.includes("Expiration")
      ) {
        // Invalid expiration/expirationTtl
        const expirationMatch = message.match(/of (\d+)/);
        return new KVExpirationError({
          key: key ?? "",
          operation,
          reason: extractReason(message),
          expirationValue: expirationMatch?.[1]
            ? parseInt(expirationMatch[1])
            : undefined,
        });
      } else if (
        message.includes("JSON") ||
        message.includes("parse") ||
        message.includes("non-json value")
      ) {
        // JSON parsing failed
        return new KVJsonParseError({
          key: key ?? "",
          operation,
          cause: error,
        });
      } else if (message.includes("cache_ttl")) {
        // Invalid cacheTtl
        const ttlMatch = message.match(/of (\d+)/);
        return new KVCacheTtlError({
          key: key ?? "",
          operation,
          reason: extractReason(message),
          cacheTtlValue: ttlMatch?.[1] ? parseInt(ttlMatch[1]) : undefined,
        });
      } else if (
        message.includes("key_count_limit") ||
        message.includes("maximum of") ||
        message.includes("minimum of")
      ) {
        // Bulk/list operation limits
        const countMatch = message.match(/of (\d+)/);
        if (operation === "list") {
          return new KVListLimitError({
            operation,
            reason: extractReason(message),
            limitValue: countMatch?.[1] ? parseInt(countMatch[1]) : undefined,
          });
        }
        return new KVBulkLimitError({
          operation,
          reason: extractReason(message),
          requestedCount: countMatch?.[1] ? parseInt(countMatch[1]) : undefined,
        });
      }
      // Other 400 errors
      return new KVNetworkError({
        key,
        operation,
        reason: extractReason(message),
        cause: error,
      });
    }

    case 404:
      // Not found - shouldn't happen as KV returns null for missing keys
      // But included for completeness
      return new KVNetworkError({
        key,
        operation,
        reason: "Key not found",
        cause: error,
      });

    default:
      // Network errors, 5xx, timeouts, unknown status
      return new KVNetworkError({
        key,
        operation,
        reason: message,
        cause: error,
      });
  }
};

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = <Key extends string = string>(
  kv: globalThis.KVNamespace<Key>,
): KVNamespace<Key> => {
  return {
    get: ((...args: unknown[]) => {
      const [keyOrKeys, typeOrOptions] = args;

      // Batch operation (array of keys)
      if (Array.isArray(keyOrKeys)) {
        const keys = keyOrKeys;
        const type =
          typeof typeOrOptions === "string"
            ? typeOrOptions
            : (typeOrOptions as any)?.type;
        const options =
          typeof typeOrOptions === "object" && typeOrOptions !== null
            ? typeOrOptions
            : undefined;

        return Effect.tryPromise({
          try: async () => {
            const result = await kv.get(keys, options ?? type);
            const effectMap = new Map<string, Option.Option<any>>();
            for (const [k, v] of result.entries()) {
              effectMap.set(k, v === null ? Option.none() : Option.some(v));
            }
            return effectMap as ReadonlyMap<string, Option.Option<any>>;
          },
          catch: (error) => mapError(error, "get"),
        });
      }

      // Single key operation
      const key = keyOrKeys as Key;
      const type =
        typeof typeOrOptions === "string"
          ? typeOrOptions
          : (typeOrOptions as any)?.type;
      const options =
        typeof typeOrOptions === "object" && typeOrOptions !== null
          ? typeOrOptions
          : undefined;

      return Effect.tryPromise({
        try: async () => {
          const result = await kv.get(key, options ?? type);
          return result === null ? Option.none() : Option.some(result);
        },
        catch: (error) => mapError(error, "get", key),
      });
    }) as KVNamespace<Key>["get"],

    list: <Metadata = unknown>(options?: ListOptions) => {
      return Effect.tryPromise({
        try: async () => {
          const rawOptions = {
            limit: Option.getOrUndefined(options?.limit ?? Option.none()),
            prefix: Option.getOrUndefined(options?.prefix ?? Option.none()),
            cursor: Option.getOrUndefined(options?.cursor ?? Option.none()),
          };
          const result = await kv.list<Metadata>(rawOptions);

          const keys: Array<ListKey<Metadata, Key>> = result.keys.map((k) => ({
            name: k.name,
            expiration:
              k.expiration !== undefined
                ? Option.some(k.expiration)
                : Option.none(),
            metadata:
              k.metadata !== undefined
                ? Option.some(k.metadata as Metadata)
                : Option.none(),
          }));

          const cacheStatus: Option.Option<string> =
            result.cacheStatus !== null
              ? Option.some(result.cacheStatus)
              : Option.none();

          if (result.list_complete) {
            return {
              listComplete: true,
              keys,
              cacheStatus,
            } as const;
          } else {
            return {
              listComplete: false,
              keys,
              cursor: result.cursor,
              cacheStatus,
            } as const;
          }
        },
        catch: (error) => mapError(error, "list"),
      });
    },

    put: (key, value, options) => {
      return Effect.tryPromise({
        try: async () => {
          const rawOptions = options
            ? {
                expiration: Option.getOrUndefined(
                  options.expiration ?? Option.none(),
                ),
                expirationTtl: Option.getOrUndefined(
                  options.expirationTtl ?? Option.none(),
                ),
                metadata: Option.getOrUndefined(
                  options.metadata ?? Option.none(),
                ),
              }
            : undefined;
          await kv.put(key, value, rawOptions);
        },
        catch: (error) => mapError(error, "put", key),
      });
    },

    getWithMetadata: ((...args: unknown[]) => {
      const [keyOrKeys, typeOrOptions] = args;

      // Batch operation (array of keys)
      if (Array.isArray(keyOrKeys)) {
        const keys = keyOrKeys;
        const type =
          typeof typeOrOptions === "string"
            ? typeOrOptions
            : (typeOrOptions as any)?.type;
        const options =
          typeof typeOrOptions === "object" && typeOrOptions !== null
            ? typeOrOptions
            : undefined;

        return Effect.tryPromise({
          try: async () => {
            const result = await kv.getWithMetadata(keys, options ?? type);
            const effectMap = new Map<
              string,
              GetWithMetadataResult<any, any>
            >();
            for (const [k, v] of result.entries()) {
              effectMap.set(k, {
                value: v.value === null ? Option.none() : Option.some(v.value),
                metadata:
                  v.metadata === null ? Option.none() : Option.some(v.metadata),
                cacheStatus:
                  v.cacheStatus === null
                    ? Option.none()
                    : Option.some(v.cacheStatus),
              });
            }
            return effectMap as ReadonlyMap<
              string,
              GetWithMetadataResult<any, any>
            >;
          },
          catch: (error) => mapError(error, "getWithMetadata"),
        });
      }

      // Single key operation
      const key = keyOrKeys as Key;
      const type =
        typeof typeOrOptions === "string"
          ? typeOrOptions
          : (typeOrOptions as any)?.type;
      const options =
        typeof typeOrOptions === "object" && typeOrOptions !== null
          ? typeOrOptions
          : undefined;

      return Effect.tryPromise({
        try: async () => {
          const result = await kv.getWithMetadata(key, options ?? type);
          return {
            value:
              result.value === null ? Option.none() : Option.some(result.value),
            metadata:
              result.metadata === null
                ? Option.none()
                : Option.some(result.metadata),
            cacheStatus:
              result.cacheStatus === null
                ? Option.none()
                : Option.some(result.cacheStatus),
          };
        },
        catch: (error) => mapError(error, "getWithMetadata", key),
      });
    }) as KVNamespace<Key>["getWithMetadata"],

    delete: (key) => {
      return Effect.tryPromise({
        try: async () => {
          await kv.delete(key);
        },
        catch: (error) => mapError(error, "delete", key),
      });
    },
  };
};

/**
 * @since 1.0.0
 * @category tags
 */
export const tag = Context.GenericTag<KVNamespace>(
  "@effect-cloudflare/KVNamespace",
);

/**
 * @since 1.0.0
 * @category layers
 */
export const layer = (kv: globalThis.KVNamespace): Layer.Layer<KVNamespace> =>
  Layer.succeed(tag, make(kv));

/**
 * @since 1.0.0
 * @category combinators
 */
export const withKVNamespace: {
  (
    kv: globalThis.KVNamespace,
  ): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    kv: globalThis.KVNamespace,
  ): Effect.Effect<A, E, R>;
} = dual(
  2,
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    kv: globalThis.KVNamespace,
  ): Effect.Effect<A, E, R> => Effect.provideService(effect, tag, make(kv)),
);
