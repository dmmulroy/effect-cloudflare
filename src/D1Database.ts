/**
 * @since 1.0.0
 */
import type * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import * as internal from "./internal/d1-database";

// Re-export all types and errors
export type {
  D1DatabaseError,
  D1Operation,
  D1Meta,
  D1Result,
  D1ExecResult,
  D1Bookmark,
  D1SessionLocationHint,
} from "./internal/d1-database";

export {
  TypeId,
  isD1DatabaseError,
  D1SQLSyntaxError,
  D1ConstraintError,
  D1TypeMismatchError,
  D1ColumnNotFoundError,
  D1ExecutionError,
  D1QueryLimitError,
  D1BatchError,
  D1NetworkError,
  D1QuotaExceededError,
  D1SessionError,
} from "./internal/d1-database";

/**
 * @since 1.0.0
 * @category models
 */
export type D1Database = internal.D1Database;

/**
 * @since 1.0.0
 * @category models
 */
export type D1PreparedStatement = internal.D1PreparedStatement;

/**
 * @since 1.0.0
 * @category models
 */
export type D1DatabaseSession = internal.D1DatabaseSession;

/**
 * @since 1.0.0
 * @category tags
 */
export const D1Database: Context.Tag<D1Database, D1Database> = internal.tag;

/**
 * @since 1.0.0
 * @category constructors
 */
export const make: (db: globalThis.D1Database) => D1Database = internal.make;

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: (
  db: globalThis.D1Database,
) => Layer.Layer<D1Database> = internal.layer;

/**
 * @since 1.0.0
 * @category combinators
 */
export const withD1Database: {
  (
    db: globalThis.D1Database,
  ): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    db: globalThis.D1Database,
  ): Effect.Effect<A, E, R>;
} = internal.withD1Database;
