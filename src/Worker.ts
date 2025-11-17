/**
 * @since 1.0.0
 */
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type { CloudflareEnv } from "./internal/env";
import type { CloudflareExecutionContext } from "./internal/context";
import * as internal from "./internal/worker";

/**
 * Creates a Cloudflare Workers fetch handler from an Effect-based handler function.
 *
 * The handler receives the request, effectified environment bindings, and execution context.
 * KVNamespace bindings are automatically converted to Effect-based implementations.
 *
 * @since 1.0.0
 * @category constructors
 * @example
 * ```typescript
 * import { Worker } from "effect-cloudflare"
 * import { Effect, Layer } from "effect"
 *
 * export default Worker.makeFetchEntryPoint(
 *   (req, env, ctx) => Effect.gen(function* () {
 *     const value = yield* env.MY_KV.get("key")
 *     return new Response("OK")
 *   }).pipe(
 *     Effect.catchAll(error =>
 *       Effect.succeed(new Response("Error", { status: 500 }))
 *     )
 *   ),
 *   { layer: Layer.empty }
 * )
 * ```
 *
 * @example
 * With custom layers:
 * ```typescript
 * export default Worker.makeFetchEntryPoint(
 *   (req, env, ctx) => Effect.gen(function* () {
 *     const db = yield* Database
 *     return new Response("OK")
 *   }).pipe(Effect.catchAll(...)),
 *   { layer: DatabaseLive }
 * )
 * ```
 */
export const makeFetchEntryPoint: <R, E>(
  handler: (
    req: Request,
    env: CloudflareEnv,
    ctx: CloudflareExecutionContext,
  ) => Effect.Effect<Response, E, R>,
  options: {
    readonly layer: Layer.Layer<R, E>;
    readonly memoMap?: Layer.MemoMap;
  },
) => ExportedHandler<Cloudflare.Env> = internal.makeFetchEntryPoint;
