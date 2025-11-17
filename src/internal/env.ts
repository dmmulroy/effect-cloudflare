import * as Effect from "effect/Effect";
import { dual } from "effect/Function";
import * as Context from "effect/Context";
import type { KVNamespace as EffectKVNamespace } from "./kv-namespace";
import { make as makeKVNamespace } from "./kv-namespace";
import type { R2Bucket as EffectR2Bucket } from "./r2-bucket";
import { make as makeR2Bucket } from "./r2-bucket";

type EffectifyBinding<Value> = [Value] extends [KVNamespace]
  ? EffectKVNamespace
  : [Value] extends [R2Bucket]
    ? EffectR2Bucket
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
    "delete" in binding &&
    !("head" in binding) // R2Bucket also has get/put/list/delete but has head
  );
}

function isR2Bucket(binding: unknown): binding is globalThis.R2Bucket {
  return (
    binding !== undefined &&
    binding !== null &&
    typeof binding === "object" &&
    "head" in binding &&
    "get" in binding &&
    "put" in binding &&
    "delete" in binding &&
    "list" in binding
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

    // Detect R2Bucket binding first (more specific - has head method)
    if (isR2Bucket(binding)) {
      effectEnv[key] = makeR2Bucket(binding);
    }
    // Detect KVNamespace binding by checking for required methods
    else if (isKVNamespace(binding)) {
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
