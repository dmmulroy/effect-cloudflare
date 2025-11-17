import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { dual, pipe } from "effect/Function";

export interface CloudflareExecutionContext {
  /**
   * Schedules an effect to run in the background using ExecutionContext.waitUntil.
   * The effect will continue running even after the response is sent.
   */
  readonly waitUntil: <A, E>(effect: Effect.Effect<A, E>) => void;

  /**
   * Allows a Worker to fail open and pass a request through to an origin server
   * when the Worker throws an unhandled exception.
   */
  readonly passThroughOnException: Effect.Effect<void>;

  /**
   * Get the raw ExecutionContext object for direct access if needed.
   */
  readonly ["~raw"]: globalThis.ExecutionContext;
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const makeExecutionContext = (
  ctx: globalThis.ExecutionContext,
): CloudflareExecutionContext => ({
  waitUntil: <A, E>(effect: Effect.Effect<A, E>) => {
    ctx.waitUntil(
      Effect.runPromise(
        pipe(effect, Effect.tapErrorCause(Effect.logError), Effect.asVoid),
      ),
    );
  },
  passThroughOnException: Effect.sync(() => ctx.passThroughOnException?.()),
  ["~raw"]: ctx,
});

/**
 * @since 1.0.0
 * @category tags
 */
export class ExecutionContext extends Context.Tag(
  "@effect-cloudflare/ExecutionContext",
)<ExecutionContext, CloudflareExecutionContext>() {}

/*
 * @since 1.0.0
 * @category combinators
 */
export const withExecutionContext: {
  (
    executionContext: globalThis.ExecutionContext,
  ): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    executionContext: globalThis.ExecutionContext,
  ): Effect.Effect<A, E, R>;
} = dual(
  2,
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    executionContext: globalThis.ExecutionContext,
  ): Effect.Effect<A, E, R> =>
    Effect.provideService(
      effect,
      ExecutionContext,
      makeExecutionContext(executionContext),
    ),
);

/**
 * @since 1.0.0
 * @category layers
 */
export const layer = (
  executionContext: globalThis.ExecutionContext,
): Layer.Layer<ExecutionContext> =>
  Layer.succeed(ExecutionContext, makeExecutionContext(executionContext));
