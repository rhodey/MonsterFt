/** With all local owners stopped, certify the databases and return their applied sequence. */
export function certify(databasePath: string, allowUnresolved?: boolean): bigint
