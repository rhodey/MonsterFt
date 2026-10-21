if (process.argv[2] === 'exit') {
  process.exit(Number(process.argv[3] ?? 0))
}
setInterval(() => {}, 60_000)
