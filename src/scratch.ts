import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Worker from "./internal/worker";
import * as Layer from "effect/Layer";

export default Worker.makeFetchEntryPoint(
  Effect.fn(function* (_req, env, ctx) {
    const maybeValue = yield* env.KV.get("last_accessed");

    ctx.waitUntil(
      Effect.gen(function* () {
        yield* Effect.log("runs after response");
        yield* env.KV.put("last_accessed", `${Date.now()}`);
      }),
    );

    return Option.match(maybeValue, {
      onNone: () =>
        new Response(JSON.stringify({ last_accessed: "Never accessed :(" }), {
          status: 404,
        }),
      onSome: (last_accessed) =>
        new Response(JSON.stringify({ last_accessed }), { status: 200 }),
    });
  }),
  { layer: Layer.empty },
);
