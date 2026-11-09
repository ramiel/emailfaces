/** Thrown when a provider (or our own limiter) says we must slow down. */
export class RateLimitedError extends Error {
  override readonly name = 'RateLimitedError';
  constructor(
    /** How long until a retry is expected to succeed, if known. */
    readonly retryAfterMs: number | undefined,
    message = `Rate limited${retryAfterMs !== undefined ? `, retry in ${Math.ceil(retryAfterMs)}ms` : ''}`,
  ) {
    super(message);
  }
}

/** Unexpected HTTP status returned by a provider. */
export class HttpError extends Error {
  override readonly name = 'HttpError';
  constructor(
    readonly status: number,
    readonly url: string,
  ) {
    super(`HTTP ${status} for ${url}`);
  }
}

export class InvalidEmailError extends Error {
  override readonly name = 'InvalidEmailError';
  constructor(readonly input: string) {
    super(`Invalid email address: ${JSON.stringify(input)}`);
  }
}
