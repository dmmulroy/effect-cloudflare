/**
 * @since 1.0.0
 */
import type * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import * as internal from "./internal/kv-namespace";

// Re-export all types and errors
export type {
  KVNamespaceError,
  KVOperation,
  ListKey,
  ListResult,
  GetOptions,
  PutOptions,
  ListOptions,
  GetWithMetadataResult,
} from "./internal/kv-namespace";

export {
  TypeId,
  isKVNamespaceError,
  KVRateLimitError,
  KVResponseTooLargeError,
  KVJsonParseError,
  KVInvalidKeyError,
  KVInvalidValueError,
  KVMetadataError,
  KVExpirationError,
  KVNetworkError,
} from "./internal/kv-namespace";

/**
 * @since 1.0.0
 * @category models
 */
export type KVNamespace<Key extends string = string> = internal.KVNamespace<Key>;

/**
 * @since 1.0.0
 * @category tags
 */
export const KVNamespace: Context.Tag<KVNamespace, KVNamespace> = internal.tag;

/**
 * @since 1.0.0
 * @category constructors
 */
export const make: <Key extends string = string>(
  kv: globalThis.KVNamespace<Key>,
) => KVNamespace<Key> = internal.make;

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: (
  kv: globalThis.KVNamespace,
) => Layer.Layer<KVNamespace> = internal.layer;

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
} = internal.withKVNamespace;
