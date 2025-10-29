#!/usr/bin/env python3
import os
import sys
import subprocess
from pathlib import Path

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <num_iterations>")
        sys.exit(1)

    num_iterations = int(sys.argv[1])
    outdir = Path("/tmp/testmany")
    outdir.mkdir(parents=True, exist_ok=True)

    passed = 0
    failed = 0
    failed_runs = []

    print(f"Running {num_iterations} iterations of 'go test -race -v -run 3A' ...\n")

    for i in range(1, num_iterations + 1):
        outfile = outdir / f"{i}.txt"
        cmd = ["go", "test", "-race", "-v", "-run", "3A"]

        with open(outfile, "w") as f:
            proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
            ret = proc.returncode

        if ret == 0:
            passed += 1
            print(f"[{i:3d}] PASS")
        else:
            failed += 1
            failed_runs.append(str(outfile))
            print(f"[{i:3d}] FAIL → {outfile}")

    print("\nSummary:")
    print(f"{num_iterations} test iterations, {passed} passed, {failed} failed")

    if failed_runs:
        print("\nThe failed test runs are:")
        for f in failed_runs:
            print(f"  {f}")

if __name__ == "__main__":
    main()

