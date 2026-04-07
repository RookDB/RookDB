#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

stamp="$(date +%Y%m%d_%H%M%S)"
out="/tmp/rookdb_type_bench_${stamp}.log"

{
  echo "RookDB Type Benchmark Run"
  echo "timestamp: $(date -Is)"
  echo "host: $(uname -srvmo)"
  echo "rustc: $(rustc --version)"
  echo "cargo: $(cargo --version)"
  echo
  cargo test --test test_type_benchmarks -- --nocapture --test-threads=1
} | tee "$out"

echo
echo "Benchmark output saved to: $out"
