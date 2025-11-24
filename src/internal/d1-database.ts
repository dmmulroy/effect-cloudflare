/**
 * @since 1.0.0
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import { dual } from "effect/Function";
import {
  type D1Database as NativeD1Database,
  type D1PreparedStatement as NativeD1PreparedStatement,
  type D1DatabaseSession as NativeD1DatabaseSession,
} from "@cloudflare/workers-types";

/**
 * @since 1.0.0
 * @category type id
 */
export const TypeId: unique symbol = Symbol.for(
  "@effect-cloudflare/D1DatabaseError",
);

/**
 * @since 1.0.0
 * @category type id
 */
export type TypeId = typeof TypeId;

/**
 * @since 1.0.0
 * @category refinements
 */
export const isD1DatabaseError = (u: unknown): u is D1DatabaseError =>
  Predicate.hasProperty(u, TypeId);

/**
 * @since 1.0.0
 * @category models
 */
export const D1Operation = Schema.Literal(
  "prepare",
  "bind",
  "first",
  "run",
  "all",
  "raw",
  "batch",
  "exec",
  "withSession",
);

/**
 * @since 1.0.0
 * @category models
 */
export type D1Operation = typeof D1Operation.Type;

/**
 * @since 1.0.0
 * @category models
 */
export const ConstraintType = Schema.Literal(
  "UNIQUE",
  "FOREIGN KEY",
  "NOT NULL",
  "CHECK",
  "PRIMARY KEY",
  "UNKNOWN",
);

/**
 * @since 1.0.0
 * @category models
 */
export type ConstraintType = typeof ConstraintType.Type;

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/observability/debug-d1/ - D1 error handling
 * @see https://www.sqlite.org/rescode.html - SQLite error codes
 *
 * Thrown when SQL contains syntax errors or is malformed.
 *
 * **Error Prefix:** `D1_EXEC_ERROR`
 *
 * **Trigger:** Invalid SQL syntax, malformed queries
 *
 * **Recovery:** Non-retriable, requires query fix
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   // This will throw D1SQLSyntaxError
 *   const stmt = db.prepare("SELCT * FROM users")
 *   yield* stmt.run()
 * }).pipe(
 *   Effect.catchTag("D1SQLSyntaxError", (error) =>
 *     Effect.log(`SQL syntax error: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class D1SQLSyntaxError extends Schema.TaggedError<D1SQLSyntaxError>(
  "@effect-cloudflare/D1DatabaseError/SQLSyntax",
)("D1SQLSyntaxError", {
  sql: Schema.String,
  operation: D1Operation,
  reason: Schema.String,
  line: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const lineMsg = this.line !== undefined ? ` (line ${this.line})` : "";
    return `D1 SQL syntax error${lineMsg} during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://www.sqlite.org/lang_createtable.html#constraints - SQLite constraints
 * @see https://www.sqlite.org/rescode.html - SQLITE_CONSTRAINT error codes
 *
 * Thrown when a constraint violation occurs (UNIQUE, FOREIGN KEY, NOT NULL, CHECK, PRIMARY KEY).
 *
 * **Error Prefix:** `D1_EXEC_ERROR`
 *
 * **Trigger:** Constraint violations during INSERT or UPDATE
 *
 * **Recovery:** Non-retriable, business logic issue
 *
 * **SQLite Error Codes:**
 * - SQLITE_CONSTRAINT_UNIQUE (2067)
 * - SQLITE_CONSTRAINT_FOREIGNKEY (787)
 * - SQLITE_CONSTRAINT_NOTNULL (1299)
 * - SQLITE_CONSTRAINT_CHECK (275)
 * - SQLITE_CONSTRAINT_PRIMARYKEY (1555)
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   // Insert duplicate email
 *   yield* db.prepare("INSERT INTO users (email) VALUES (?)")
 *     .bind("test@example.com")
 *     .run()
 *
 *   // This will throw D1ConstraintError with constraintType: "UNIQUE"
 *   yield* db.prepare("INSERT INTO users (email) VALUES (?)")
 *     .bind("test@example.com")
 *     .run()
 * }).pipe(
 *   Effect.catchTag("D1ConstraintError", (error) =>
 *     Effect.gen(function* () {
 *       if (error.constraintType === "UNIQUE") {
 *         yield* Effect.log("Duplicate entry")
 *       }
 *     })
 *   )
 * )
 * ```
 */
export class D1ConstraintError extends Schema.TaggedError<D1ConstraintError>(
  "@effect-cloudflare/D1DatabaseError/Constraint",
)("D1ConstraintError", {
  sql: Schema.String,
  operation: D1Operation,
  constraintType: ConstraintType,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `D1 ${this.constraintType} constraint violation during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/observability/debug-d1/ - "Type mismatch" errors
 *
 * Thrown when using undefined instead of null, or other type mismatches.
 *
 * **Error Prefix:** `D1_TYPE_ERROR`
 *
 * **Trigger:** Using `undefined` instead of `null` in bind parameters
 *
 * **Recovery:** Non-retriable, requires proper null handling
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   const maybeValue: string | undefined = undefined
 *
 *   // This will throw D1TypeMismatchError
 *   yield* db.prepare("INSERT INTO users (name) VALUES (?)")
 *     .bind(maybeValue)  // Should use null instead
 *     .run()
 * }).pipe(
 *   Effect.catchTag("D1TypeMismatchError", (error) =>
 *     Effect.log("Use null instead of undefined")
 *   )
 * )
 *
 * // Correct usage:
 * const correctProgram = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   const maybeValue: string | null = null
 *
 *   yield* db.prepare("INSERT INTO users (name) VALUES (?)")
 *     .bind(maybeValue)
 *     .run()
 * })
 * ```
 */
export class D1TypeMismatchError extends Schema.TaggedError<D1TypeMismatchError>(
  "@effect-cloudflare/D1DatabaseError/TypeMismatch",
)("D1TypeMismatchError", {
  sql: Schema.String,
  operation: D1Operation,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `D1 type mismatch during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/worker-api/prepared-statements/ - first() method
 *
 * Thrown when referencing a non-existent column, especially in first(columnName).
 *
 * **Error Prefix:** `D1_COLUMN_NOTFOUND`
 *
 * **Trigger:** Calling first(columnName) with invalid column
 *
 * **Recovery:** Non-retriable, query/schema issue
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   const stmt = db.prepare("SELECT id, name FROM users WHERE id = ?")
 *     .bind(1)
 *
 *   // This will throw D1ColumnNotFoundError
 *   const age = yield* stmt.first<number>("age")  // Column doesn't exist
 * }).pipe(
 *   Effect.catchTag("D1ColumnNotFoundError", (error) =>
 *     Effect.log(`Column not found: ${error.columnName}`)
 *   )
 * )
 * ```
 */
export class D1ColumnNotFoundError extends Schema.TaggedError<D1ColumnNotFoundError>(
  "@effect-cloudflare/D1DatabaseError/ColumnNotFound",
)("D1ColumnNotFoundError", {
  sql: Schema.String,
  operation: D1Operation,
  columnName: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `D1 column not found "${this.columnName}" during ${this.operation}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/observability/debug-d1/ - D1_EXEC_ERROR
 *
 * Thrown for general SQL execution errors (not syntax or constraint).
 *
 * **Error Prefix:** `D1_EXEC_ERROR`
 *
 * **Trigger:** Runtime SQL errors that aren't syntax or constraint violations
 *
 * **Recovery:** Depends on specific error
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   yield* db.prepare("SELECT * FROM non_existent_table").run()
 * }).pipe(
 *   Effect.catchTag("D1ExecutionError", (error) =>
 *     Effect.log(`Execution error: ${error.reason}`)
 *   )
 * )
 * ```
 */
export class D1ExecutionError extends Schema.TaggedError<D1ExecutionError>(
  "@effect-cloudflare/D1DatabaseError/Execution",
)("D1ExecutionError", {
  sql: Schema.String,
  operation: D1Operation,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `D1 execution error during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/platform/limits/ - D1 limits
 *
 * Thrown when exceeding D1 query limits.
 *
 * **Limits:**
 * - Queries per invocation: 50 (Free) / 1000 (Paid)
 * - SQL statement length: 100KB
 * - Bound parameters: 100 per query
 * - Query duration: 30 seconds
 *
 * **Recovery:** Partially retriable (split queries, reduce complexity)
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   // Too many parameters
 *   const params = Array.from({ length: 101 }, (_, i) => i)
 *   const placeholders = params.map(() => "?").join(",")
 *   const stmt = db.prepare(`SELECT * FROM users WHERE id IN (${placeholders})`)
 *
 *   yield* params.reduce(
 *     (s, p) => s.bind(p),
 *     stmt
 *   ).run()
 * }).pipe(
 *   Effect.catchTag("D1QueryLimitError", (error) =>
 *     Effect.log(`Query limit exceeded: ${error.limitType}`)
 *   )
 * )
 * ```
 */
export class D1QueryLimitError extends Schema.TaggedError<D1QueryLimitError>(
  "@effect-cloudflare/D1DatabaseError/QueryLimit",
)("D1QueryLimitError", {
  sql: Schema.String,
  operation: D1Operation,
  reason: Schema.String,
  limitType: Schema.Literal(
    "queries_per_invocation",
    "statement_length",
    "bound_parameters",
    "query_duration",
  ),
  currentValue: Schema.optional(Schema.Number),
  maxValue: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const limitMsg =
      this.currentValue !== undefined && this.maxValue !== undefined
        ? ` (${this.currentValue}/${this.maxValue})`
        : "";
    return `D1 query limit exceeded: ${this.limitType}${limitMsg} - ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/worker-api/d1-database/ - batch() method
 *
 * Thrown when a batch operation fails. Entire batch is rolled back atomically.
 *
 * **Error Prefix:** `D1_ERROR` or `D1_EXEC_ERROR`
 *
 * **Trigger:** Any statement failure in batch
 *
 * **Recovery:** Retriable, but entire batch rolls back on failure
 *
 * **Important:** D1 batch operations are atomic SQL transactions. If any statement
 * fails, the entire batch is rolled back and no changes are committed.
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   const stmt1 = db.prepare("INSERT INTO users (id, name) VALUES (?, ?)").bind(1, "Alice")
 *   const stmt2 = db.prepare("INVALID SQL")  // This fails
 *   const stmt3 = db.prepare("INSERT INTO users (id, name) VALUES (?, ?)").bind(2, "Bob")
 *
 *   // Entire batch rolls back - neither Alice nor Bob inserted
 *   yield* db.batch([stmt1, stmt2, stmt3])
 * }).pipe(
 *   Effect.catchTag("D1BatchError", (error) =>
 *     Effect.log("Batch failed, all changes rolled back")
 *   )
 * )
 * ```
 */
export class D1BatchError extends Schema.TaggedError<D1BatchError>(
  "@effect-cloudflare/D1DatabaseError/Batch",
)("D1BatchError", {
  operation: D1Operation,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `D1 batch operation failed (all changes rolled back): ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/observability/debug-d1/ - Transient errors
 *
 * Thrown for network-level errors, timeouts, or transient failures.
 *
 * **Error Prefix:** `D1_ERROR`
 *
 * **Common scenarios:**
 * - Network connectivity issues
 * - "Network connection lost"
 * - "Cannot resolve D1 due to transient issue on remote node"
 * - "internal error"
 * - Database code updates (temporary unavailability)
 * - Request queue timeouts
 *
 * **Recovery:** Retriable with exponential backoff for transient errors
 *
 * **Note:** D1 automatically retries read-only queries (SELECT, EXPLAIN, WITH)
 * up to 2 times on retryable failures.
 *
 * @example
 * ```typescript
 * import { Effect, Schedule } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *   yield* db.prepare("SELECT * FROM users").run()
 * }).pipe(
 *   Effect.catchTag("D1NetworkError", (error) =>
 *     Effect.gen(function* () {
 *       if (error.isTransient) {
 *         yield* Effect.log("Transient error, will retry")
 *         return yield* Effect.fail(error)
 *       }
 *       yield* Effect.log(`Permanent network error: ${error.reason}`)
 *     })
 *   ),
 *   Effect.retry(
 *     Schedule.exponential("100 millis", 2).pipe(
 *       Schedule.compose(Schedule.recurs(3))
 *     )
 *   )
 * )
 * ```
 */
export class D1NetworkError extends Schema.TaggedError<D1NetworkError>(
  "@effect-cloudflare/D1DatabaseError/Network",
)("D1NetworkError", {
  sql: Schema.optional(Schema.String),
  operation: D1Operation,
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect),
  isTransient: Schema.Boolean,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const sqlMsg = this.sql ? ` for query "${this.sql}"` : "";
    const transientMsg = this.isTransient ? " (transient)" : "";
    return `D1 network error${sqlMsg} during ${this.operation}${transientMsg}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/platform/limits/ - D1 quotas
 *
 * Thrown when account or database quotas are exceeded.
 *
 * **Limits:**
 * - Database size: 500MB (Free) / 10GB (Paid)
 * - Storage per account: 5GB (Free) / 1TB (Paid)
 * - Row/BLOB size: 2MB maximum
 * - Databases per account: 10 (Free) / 50,000 (Paid)
 *
 * **Recovery:** Non-retriable, requires plan upgrade or data cleanup
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   // Insert large BLOB exceeding 2MB
 *   const largeData = new Uint8Array(3 * 1024 * 1024)
 *   yield* db.prepare("INSERT INTO files (data) VALUES (?)")
 *     .bind(Array.from(largeData))
 *     .run()
 * }).pipe(
 *   Effect.catchTag("D1QuotaExceededError", (error) =>
 *     Effect.log(`Quota exceeded: ${error.quotaType}`)
 *   )
 * )
 * ```
 */
export class D1QuotaExceededError extends Schema.TaggedError<D1QuotaExceededError>(
  "@effect-cloudflare/D1DatabaseError/QuotaExceeded",
)("D1QuotaExceededError", {
  operation: D1Operation,
  reason: Schema.String,
  quotaType: Schema.Literal(
    "database_size",
    "account_storage",
    "row_size",
    "database_count",
  ),
  currentValue: Schema.optional(Schema.Number),
  maxValue: Schema.optional(Schema.Number),
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    const quotaMsg =
      this.currentValue !== undefined && this.maxValue !== undefined
        ? ` (${this.currentValue}/${this.maxValue})`
        : "";
    return `D1 quota exceeded: ${this.quotaType}${quotaMsg} - ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category errors
 * @see https://developers.cloudflare.com/d1/best-practices/read-replication/ - Sessions
 *
 * Thrown when session or bookmark operations fail.
 *
 * **Error Prefix:** `D1_SESSION_ERROR`
 *
 * **Trigger:** Invalid bookmark or session constraint
 *
 * **Recovery:** May be retriable with new session
 *
 * @example
 * ```typescript
 * import { Effect } from "effect"
 * import * as D1 from "@effect-cloudflare/D1Database"
 *
 * const program = Effect.gen(function* () {
 *   const db = yield* D1.D1Database
 *
 *   const invalidBookmark = "invalid-bookmark-string"
 *
 *   // This will throw D1SessionError
 *   const [result, bookmark] = yield* db.withSession(invalidBookmark, (session) =>
 *     Effect.gen(function* () {
 *       return yield* session.prepare("SELECT 1").first()
 *     })
 *   )
 * }).pipe(
 *   Effect.catchTag("D1SessionError", (error) =>
 *     Effect.log("Invalid session, creating new session")
 *   )
 * )
 * ```
 */
export class D1SessionError extends Schema.TaggedError<D1SessionError>(
  "@effect-cloudflare/D1DatabaseError/Session",
)("D1SessionError", {
  operation: D1Operation,
  reason: Schema.String,
}) {
  /**
   * @since 1.0.0
   */
  readonly [TypeId]: typeof TypeId = TypeId;

  /**
   * @since 1.0.0
   */
  override get message(): string {
    return `D1 session error during ${this.operation}: ${this.reason}`;
  }
}

/**
 * @since 1.0.0
 * @category models
 */
export type D1DatabaseError =
  | D1SQLSyntaxError
  | D1ConstraintError
  | D1TypeMismatchError
  | D1ColumnNotFoundError
  | D1ExecutionError
  | D1QueryLimitError
  | D1BatchError
  | D1NetworkError
  | D1QuotaExceededError
  | D1SessionError;

/**
 * @since 1.0.0
 * @category models
 */
export interface D1Meta {
  readonly served_by: string;
  readonly duration: number;
  readonly changes: number;
  readonly last_row_id: number;
  readonly changed_db: boolean;
  readonly size_after: number;
  readonly rows_read: number;
  readonly rows_written: number;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface D1Result<T = unknown> {
  readonly results: ReadonlyArray<T>;
  readonly success: true;
  readonly meta: D1Meta;
}

/**
 * @since 1.0.0
 * @category models
 */
export interface D1ExecResult {
  readonly count: number;
  readonly duration: number;
}

/**
 * @since 1.0.0
 * @category models
 */
export type D1Bookmark = string;

/**
 * @since 1.0.0
 * @category models
 */
export const D1SessionLocationHint = Schema.Literal(
  "first-unconstrained",
  "first-primary",
);

/**
 * @since 1.0.0
 * @category models
 */
export type D1SessionLocationHint = typeof D1SessionLocationHint.Type;

/**
 * @since 1.0.0
 * @category models
 * @see https://developers.cloudflare.com/d1/worker-api/prepared-statements/
 */
export interface D1PreparedStatement {
  readonly bind: (
    ...values: ReadonlyArray<string | number | boolean | null | number[]>
  ) => D1PreparedStatement;

  readonly first: {
    <T = unknown>(): Effect.Effect<Option.Option<T>, D1DatabaseError>;
    <T = unknown>(
      columnName: string,
    ): Effect.Effect<Option.Option<T>, D1DatabaseError>;
  };

  readonly run: <T = unknown>() => Effect.Effect<D1Result<T>, D1DatabaseError>;

  readonly all: <T = unknown>() => Effect.Effect<D1Result<T>, D1DatabaseError>;

  readonly raw: {
    <T = unknown>(options: {
      readonly columnNames: true;
    }): Effect.Effect<ReadonlyArray<[string[], ...T[]]>, D1DatabaseError>;
    <T = unknown>(options?: {
      readonly columnNames: false;
    }): Effect.Effect<ReadonlyArray<ReadonlyArray<T>>, D1DatabaseError>;
  };

  readonly "~raw": NativeD1PreparedStatement;
}

/**
 * @since 1.0.0
 * @category models
 */
export type D1SessionConstraintOrBookmark =
  | "first-primary"
  | "first-unconstrained"
  | (string & {});

/**
 * @since 1.0.0
 * @category models
 */
export interface D1DatabaseSession {
  readonly prepare: (sql: string) => D1PreparedStatement;
  batch<T = unknown>(
    statements: D1PreparedStatement[],
  ): Effect.Effect<ReadonlyArray<D1Result<T>>, D1DatabaseError>;
  /**
   * @returns The latest session bookmark across all executed queries on the session.
   *          If no query has been executed yet, `None` is returned.
   */
  getBookmark(): Effect.Effect<Option.Option<D1SessionBookmark>>;
  "~raw": NativeD1DatabaseSession;
}

/**
 * @since 1.0.0
 * @category models
 * @see https://developers.cloudflare.com/d1/worker-api/
 */
export interface D1Database {
  readonly prepare: (sql: string) => D1PreparedStatement;

  readonly batch: <T = unknown>(
    statements: ReadonlyArray<D1PreparedStatement>,
  ) => Effect.Effect<ReadonlyArray<D1Result<T>>, D1DatabaseError>;

  readonly exec: (sql: string) => Effect.Effect<D1ExecResult, D1DatabaseError>;

  readonly withSession: (
    constraintOrBookmark: D1SessionConstraintOrBookmark,
  ) => Effect.Effect<D1DatabaseSession, D1DatabaseError>;
}

/**
 * @internal
 * @category error mapping
 *
 * Helper to extract clean reason from error message
 */
const extractReason = (message: string): string => {
  // Remove D1_ERROR prefix
  const cleaned = message.replace(/^D1_\w+:\s*/, "");
  // Remove "Error in line N: statement: " prefix from D1_EXEC_ERROR
  const execMatch = cleaned.match(/Error in line \d+: .+?: (.+)/);
  return execMatch?.[1] ?? cleaned;
};

/**
 * @internal
 * @category error mapping
 */
const extractConstraintType = (message: string): ConstraintType => {
  if (/UNIQUE constraint failed/i.test(message)) return "UNIQUE";
  if (/NOT NULL constraint failed/i.test(message)) return "NOT NULL";
  if (/CHECK constraint failed/i.test(message)) return "CHECK";
  if (/PRIMARY KEY constraint failed/i.test(message)) return "PRIMARY KEY";
  if (/FOREIGN KEY constraint failed/i.test(message)) return "FOREIGN KEY";
  return "UNKNOWN";
};

/**
 * @internal
 * @category error mapping
 */
const isTransientError = (message: string): boolean => {
  return /network|connection|transient|timeout|temporary/i.test(message);
};

/**
 * @since 1.0.0
 * @category error mapping
 * @see https://github.com/cloudflare/workerd/blob/main/src/cloudflare/internal/d1-api.ts - Error implementation
 *
 * Maps native D1 errors to typed error classes.
 *
 * @param error - The caught error from D1 operation
 * @param operation - The D1 operation that failed
 * @param sql - Optional SQL query involved
 * @returns Typed D1DatabaseError
 */
const mapError = (
  error: unknown,
  operation: D1Operation,
  sql?: string,
): D1DatabaseError => {
  const message = (error as Error)?.message ?? String(error);

  // Check for constraint violations (with or without D1_EXEC_ERROR prefix)
  if (/constraint failed/i.test(message)) {
    return new D1ConstraintError({
      sql: sql ?? "",
      operation,
      constraintType: extractConstraintType(message),
      reason: extractReason(message),
    });
  }

  // Check for syntax errors (with or without D1_EXEC_ERROR prefix)
  if (/syntax error/i.test(message)) {
    const lineMatch = message.match(/line (\d+)/);
    return new D1SQLSyntaxError({
      sql: sql ?? "",
      operation,
      reason: extractReason(message),
      line: lineMatch?.[1] ? parseInt(lineMatch[1]) : undefined,
    });
  }

  // D1_TYPE_ERROR: Type mismatches
  if (
    message.startsWith("D1_TYPE_ERROR:") ||
    /type.*not supported/i.test(message)
  ) {
    return new D1TypeMismatchError({
      sql: sql ?? "",
      operation,
      reason: extractReason(message),
    });
  }

  // D1_COLUMN_NOTFOUND: Column not found
  if (
    message.startsWith("D1_COLUMN_NOTFOUND:") ||
    /column not found/i.test(message)
  ) {
    const colMatch =
      message.match(/\(([^)]+)\)/) ||
      message.match(/column not found[:\s]+([^\s]+)/i);
    return new D1ColumnNotFoundError({
      sql: sql ?? "",
      operation,
      columnName: colMatch?.[1] ?? "",
    });
  }

  // D1_SESSION_ERROR: Session errors
  if (
    message.startsWith("D1_SESSION_ERROR:") ||
    /session.*bookmark/i.test(message)
  ) {
    return new D1SessionError({
      operation,
      reason: extractReason(message),
    });
  }

  // Check for query limit errors
  if (
    /queries per invocation|statement length|bound parameters|query duration/i.test(
      message,
    )
  ) {
    let limitType:
      | "queries_per_invocation"
      | "statement_length"
      | "bound_parameters"
      | "query_duration" = "queries_per_invocation";
    if (/statement length/i.test(message)) limitType = "statement_length";
    else if (/bound parameters/i.test(message)) limitType = "bound_parameters";
    else if (/query duration/i.test(message)) limitType = "query_duration";

    return new D1QueryLimitError({
      sql: sql ?? "",
      operation,
      reason: extractReason(message),
      limitType,
    });
  }

  // Check for quota errors
  if (
    /database size|storage|row size|blob size|database count/i.test(message)
  ) {
    let quotaType:
      | "database_size"
      | "account_storage"
      | "row_size"
      | "database_count" = "database_size";
    if (/storage/i.test(message)) quotaType = "account_storage";
    else if (/row size|blob size/i.test(message)) quotaType = "row_size";
    else if (/database count/i.test(message)) quotaType = "database_count";

    return new D1QuotaExceededError({
      operation,
      reason: extractReason(message),
      quotaType,
    });
  }

  // Batch errors
  if (operation === "batch") {
    return new D1BatchError({
      operation,
      reason: extractReason(message),
    });
  }

  // SQLITE errors without D1 prefix
  if (/SQLITE_/i.test(message)) {
    return new D1ExecutionError({
      sql: sql ?? "",
      operation,
      reason: extractReason(message),
    });
  }

  // Generic D1_ERROR or unknown - network error
  return new D1NetworkError({
    sql,
    operation,
    reason: extractReason(message),
    cause: error,
    isTransient: isTransientError(message),
  });
};

/**
 * @internal
 */
const transformMeta = (meta: any): D1Meta => ({
  served_by: meta.served_by ?? "",
  duration: meta.duration ?? 0,
  changes: meta.changes ?? 0,
  last_row_id: meta.last_row_id ?? 0,
  changed_db: meta.changed_db ?? false,
  size_after: meta.size_after ?? 0,
  rows_read: meta.rows_read ?? 0,
  rows_written: meta.rows_written ?? 0,
});

/**
 * @internal
 */
const makePreparedStatement = (
  stmt: NativeD1PreparedStatement,
  sql: string,
): D1PreparedStatement => {
  return {
    bind: (...values) => {
      const bound = stmt.bind(...values);
      return makePreparedStatement(bound, sql);
    },

    first: <T>(...args: unknown[]) => {
      const columnName = args[0] as string | undefined;
      return Effect.tryPromise({
        try: async () => {
          const result =
            columnName !== undefined
              ? await stmt.first<T>(columnName)
              : await stmt.first<T>();
          return Option.fromNullable(result);
        },
        catch: (error) => mapError(error, "first", sql),
      });
    },

    run: <T>() =>
      Effect.tryPromise({
        try: async () => {
          const result = await stmt.run<T>();
          return {
            results: result.results ?? [],
            success: true,
            meta: transformMeta(result.meta),
          } as const;
        },
        catch: (error) => mapError(error, "run", sql),
      }),

    all: <T>() =>
      Effect.tryPromise({
        try: async () => {
          const result = await stmt.all<T>();
          return {
            results: result.results ?? [],
            success: true,
            meta: transformMeta(result.meta),
          } as const;
        },
        catch: (error) => mapError(error, "all", sql),
      }),

    raw: <T>(options?: { columnNames: boolean }) => {
      return Effect.tryPromise({
        try: async () => {
          // @ts-expect-error overload type inference
          return stmt.raw<T>(options);
        },
        catch: (error) => mapError(error, "raw", sql),
      });
    },

    "~raw": stmt,
  } as const satisfies D1PreparedStatement;
};

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = (db: NativeD1Database): D1Database => {
  return {
    prepare: (sql: string) => makePreparedStatement(db.prepare(sql), sql),

    batch: <T>(statements: ReadonlyArray<D1PreparedStatement>) =>
      Effect.tryPromise({
        try: async () => {
          const native = statements.map((s) => s["~raw"]);
          const results = await db.batch<T>(native);
          return results.map((r) => ({
            results: r.results ?? [],
            success: true as const,
            meta: transformMeta(r.meta),
          }));
        },
        catch: (error) => mapError(error, "batch"),
      }),

    exec: (sql: string) =>
      Effect.tryPromise({
        try: async () => {
          const result = await db.exec(sql);
          return {
            count: result.count,
            duration: result.duration,
          };
        },
        catch: (error) => mapError(error, "exec", sql),
      }),

    withSession: (constraintOrBookmark) => {
      return Effect.try({
        try: () => {
          const session = db.withSession(constraintOrBookmark);

          return {
            prepare: (sql: string) =>
              makePreparedStatement(session.prepare(sql), sql),
            batch: <T>(statements: D1PreparedStatement[]) => {
              return Effect.tryPromise({
                try: async () => {
                  const native = statements.map((s) => s["~raw"]);
                  const results = await db.batch<T>(native);
                  return results.map(
                    (r) =>
                      ({
                        results: r.results ?? [],
                        success: true,
                        meta: transformMeta(r.meta),
                      }) as const,
                  );
                },
                catch: (error) => mapError(error, "batch"),
              });
            },
            getBookmark: () =>
              Effect.sync(() => Option.fromNullable(session.getBookmark())),

            "~raw": session,
          };
        },
        catch: (error) => mapError(error, "withSession"),
      });
    },
  };
};

/**
 * @since 1.0.0
 * @category tags
 */
export const tag = Context.GenericTag<D1Database>(
  "@effect-cloudflare/D1Database",
);

/**
 * @since 1.0.0
 * @category layers
 */
export const layer = (db: NativeD1Database): Layer.Layer<D1Database> =>
  Layer.succeed(tag, make(db));

/**
 * @since 1.0.0
 * @category combinators
 */
export const withD1Database: {
  (
    db: NativeD1Database,
  ): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    db: NativeD1Database,
  ): Effect.Effect<A, E, R>;
} = dual(
  2,
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    db: NativeD1Database,
  ): Effect.Effect<A, E, R> => Effect.provideService(effect, tag, make(db)),
);
