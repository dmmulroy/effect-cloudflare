import * as Effect from "effect/Effect";
import { dual } from "effect/Function";
import * as Context from "effect/Context";
import type { KVNamespace as EffectKVNamespace } from "./kv-namespace";
import { make as makeKVNamespace } from "./kv-namespace";

type EffectifyBinding<Value> = [Value] extends [KVNamespace]
  ? EffectKVNamespace
  : Value;

type EffectifyEnv<Env = Cloudflare.Env> = {
  [Binding in keyof Env]: EffectifyBinding<Env[Binding]>;
};

export interface CloudflareEnv extends EffectifyEnv<Cloudflare.Env> {
  ["~raw"]: Cloudflare.Env;
}

function isKVNamespace(binding: unknown): binding is globalThis.KVNamespace {
  return (
    binding !== undefined &&
    binding !== null &&
    typeof binding === "object" &&
    "get" in binding &&
    "put" in binding &&
    "list" in binding &&
    "delete" in binding
  );
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const makeEnv = (env: Cloudflare.Env): CloudflareEnv => {
  const effectEnv: Record<string, unknown> = { ["~raw"]: env };

  for (const key in env) {
    const binding = env[key as keyof typeof env];

    // Detect KVNamespace binding by checking for required methods
    if (isKVNamespace(binding)) {
      effectEnv[key] = makeKVNamespace(binding);
    } else {
      effectEnv[key] = binding;
    }
  }

  // Safe conversion: we've correctly transformed all bindings
  return effectEnv as unknown as CloudflareEnv;
};

/**
 * @since 1.0.0
 * @category tags
 */
export class Env extends Context.Tag("@effect-cloudflare/Env")<
  Env,
  CloudflareEnv
>() {}

/**
 * @since 1.0.0
 * @category combinators
 */
export const withEnv: {
  (
    env: Record<string, unknown>,
  ): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    env: Cloudflare.Env,
  ): Effect.Effect<A, E, R>;
} = dual(
  2,
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    env: Cloudflare.Env,
  ): Effect.Effect<A, E, R> =>
    Effect.provideService(effect, Env, env as unknown as CloudflareEnv),
);
