/**
 * @since 1.0.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import { makeEnv, Env, type CloudflareEnv } from "./env";
import {
  makeExecutionContext,
  ExecutionContext,
  type CloudflareExecutionContext,
} from "./context";

/**
 * @since 1.0.0
 * @category constructors
 */
export function makeFetchEntryPoint<R, E>(
  handler: (
    req: Request,
    env: CloudflareEnv,
    ctx: CloudflareExecutionContext,
  ) => Effect.Effect<Response, E, R>,
  options: {
    readonly layer: Layer.Layer<R, E>;
    readonly memoMap?: Layer.MemoMap;
  },
): ExportedHandler<Cloudflare.Env> {
  const runtime = ManagedRuntime.make(options.layer, options.memoMap);

  const fetch = async (
    req: Request,
    env: Cloudflare.Env,
    ctx: globalThis.ExecutionContext,
  ): Promise<Response> => {
    const effectEnv = makeEnv(env);
    const effectCtx = makeExecutionContext(ctx);

    const context = Context.make(Env, effectEnv).pipe(
      Context.add(ExecutionContext, effectCtx),
    );

    const effect = handler(req, effectEnv, effectCtx);

    return runtime.runPromise(Effect.provide(effect, context));
  };

  return { fetch };
}
