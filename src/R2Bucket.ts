/**
 * @since 1.0.0
 */
import type * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import * as internal from "./internal/r2-bucket";

// Re-export all types and errors
export type {
  R2BucketError,
  R2Operation,
  R2Object,
  R2ObjectBody,
  R2MultipartUpload,
} from "./internal/r2-bucket";

export {
  TypeId,
  isR2BucketError,
  R2RateLimitError,
  R2ConcurrencyError,
  R2ObjectTooLargeError,
  R2InvalidKeyError,
  R2MetadataError,
  R2PreconditionFailedError,
  R2MultipartError,
  R2BucketNotFoundError,
  R2NotEnabledError,
  R2AuthorizationError,
  R2NetworkError,
} from "./internal/r2-bucket";

/**
 * @since 1.0.0
 * @category models
 */
export type R2Bucket = internal.R2Bucket;

/**
 * @since 1.0.0
 * @category tags
 */
export const R2Bucket: Context.Tag<R2Bucket, R2Bucket> = internal.R2Bucket;

/**
 * @since 1.0.0
 * @category constructors
 */
export const make: (bucket: globalThis.R2Bucket) => R2Bucket = internal.make;

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: (bucket: globalThis.R2Bucket) => Layer.Layer<R2Bucket> =
  internal.layer;

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
} = internal.withR2Bucket;
