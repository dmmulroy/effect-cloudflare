/**
 * @since 1.0.0
 */
import type { UnknownException } from "effect/Cause";
// import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { globalValue } from "effect/GlobalValue";
// import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import type { PlatformProxy } from "wrangler";
// import * as internalContext from "./context.js"

type SignalHandler = NodeJS.SignalsListener;

const wranglerSignals: ReadonlyArray<NodeJS.Signals> = [
  "SIGINT",
  "SIGTERM",
] as const;

const baselineListeners = globalValue(
  "@effect-cloudflare/wrangler/baselineListeners",
  () => {
    const map = new Map<NodeJS.Signals, Set<SignalHandler>>();
    for (const signal of wranglerSignals) {
      const listeners = new Set<SignalHandler>();
      for (const listener of process.listeners(signal)) {
        listeners.add(listener);
      }
      map.set(signal, listeners);
    }
    return map;
  },
);

const originalToWrapped = globalValue(
  "@effect-cloudflare/wrangler/originalToWrapped",
  () => new WeakMap<SignalHandler, SignalHandler>(),
);

const wrappedToOriginal = globalValue(
  "@effect-cloudflare/wrangler/wrappedToOriginal",
  () => new WeakMap<SignalHandler, SignalHandler>(),
);

/**
 * Wraps Wrangler's signal handlers to prevent immediate process.exit() and allow
 * Effect cleanup to complete.
 *
 * @internal
 */
const wrapSignalHandler = (handler: SignalHandler): SignalHandler => {
  const existing = originalToWrapped.get(handler);

  if (existing) {
    return existing;
  }

  const wrapped: SignalHandler = (signal) => {
    const originalExit = process.exit;

    // Prevent process.exit from terminating immediately
    process.exit = ((_code?: number) => {
      return undefined as never;
    }) as typeof process.exit;

    try {
      handler(signal);
    } finally {
      process.exit = originalExit;
    }
  };

  originalToWrapped.set(handler, wrapped);
  wrappedToOriginal.set(wrapped, handler);

  return wrapped;
};

/**
 * Patches Wrangler's SIGINT/SIGTERM handlers to prevent immediate process.exit().
 * This allows Effect's cleanup logic to run before the process terminates.
 *
 * @since 1.0.0
 * @category wrangler
 * @internal
 */
export const applySignalPatch = (): void => {
  for (const signal of wranglerSignals) {
    const baseline = baselineListeners.get(signal);

    for (const listener of process.listeners(signal)) {
      if (wrappedToOriginal.has(listener) || baseline?.has(listener)) {
        continue;
      }

      const wrapped = wrapSignalHandler(listener);
      process.removeListener(signal, listener);
      process.on(signal, wrapped);
    }
  }
};

/**
 * Creates a scoped platform proxy using Wrangler's getPlatformProxy.
 * Automatically disposes the proxy when the scope ends.
 *
 * @since 1.0.0
 * @category wrangler
 * @internal
 */
export const makePlatformProxy = (options?: {
  readonly configPath?: string;
  readonly persist?: boolean | { readonly path: string };
  readonly environment?: string;
}): Effect.Effect<
  PlatformProxy<Cloudflare.Env>,
  UnknownException,
  Scope.Scope
> =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const { getPlatformProxy } = yield* Effect.promise(
        () => import("wrangler"),
      );

      const proxy = yield* Effect.tryPromise(() =>
        getPlatformProxy<Cloudflare.Env>({
          ...(options?.configPath && { configPath: options.configPath }),
          ...(options?.persist && { persist: options.persist }),
          ...(options?.environment && { environment: options.environment }),
        }),
      );

      applySignalPatch();

      return proxy;
    }),
    (proxy) =>
      Effect.promise(() => proxy.dispose()).pipe(
        Effect.tapErrorCause(Effect.logWarning),
        Effect.ignore,
      ),
  );

/**
 * Creates a Layer providing Wrangler platform proxy bindings via CloudflareContext services.
 *
 * @since 1.0.0
 * @category wrangler
 * @internal
 */
// export const layer = (options?: {
//   readonly configPath?: string;
//   readonly persist?: boolean | { readonly path: string };
//   readonly environment?: string;
// }): Layer.Layer<
//   internalContext.ExecutionContext | internalContext.Env,
//   unknown,
//   never
// > =>
//   Layer.scopedContext(
//     Effect.map(makePlatformProxy(options), (proxy) =>
//       Context.make(
//         internalContext.ExecutionContext,
//         internalContext.makeExecutionContext(proxy.ctx),
//       ).pipe(Context.add(internalContext.Env, proxy.env)),
//     ),
//   );
