# Type declarations

Run `npm run test:types` from the repository root after installing dependencies.
The check packs MonsterFt, installs that tarball in a temporary ESM project, and
compiles `consumer.ts` with strict NodeNext resolution and declaration checking.
The temporary install prefers npm's cache but may need registry access; it does
not run dependency install scripts.

Keep declarations beside the public JS modules and re-export public types from
`src/index.d.ts`. Update the consumer's positive and `@ts-expect-error` cases when
changing an API. The check also compares runtime exports and error-code values
with their declarations. Run the ordinary JS suite too: declaration tests do not
type-check the implementation.
