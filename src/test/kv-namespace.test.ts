import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Wrangler from "wrangler";
import { Env, makeEnv } from "../internal/env";
import * as Option from "effect/Option";

const platformProxyLayer = Layer.scoped(
  Env,
  Effect.gen(function* () {
    const { env } = yield* Effect.acquireRelease(
      Effect.tryPromise(() => Wrangler.getPlatformProxy<Cloudflare.Env>()),
      (proxy) => Effect.tryPromise(proxy.dispose).pipe(Effect.orDie),
    );

    return makeEnv(env);
  }),
);

describe("kv-namespace", () => {
  it.effect("it should store and retrieve a value for a given key", () =>
    Effect.gen(function* () {
      const env = yield* Env;
      yield* env.KV.put("foobar", "baz");

      const value = yield* env.KV.get("foobar");

      expect(Option.getOrThrow(value)).toEqual("baz");
    }).pipe(Effect.provide(platformProxyLayer)),
  );
});
