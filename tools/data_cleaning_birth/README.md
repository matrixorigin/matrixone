# Isolated birth-fill deployment

This launcher is mandatory for each qualified launch, restart and same-binary
recovery of the DEFAULT false candidate. It does not schedule experiments.
Only a static `mo-service -launch` process with one LOG, one TN and one or more
in-process CN roles is supported. Dynamic children, remote object stores,
external recovery binaries, rolling upgrades and downgrade are unsupported.

Before preparation, independently verify the release binary's full source,
build tags/flags, toolchain and canonical native receipt. `native_sha256` binds
the receipt file, not a fresh inspection of native code inside an executable.
All CN/TN/background work in this launch uses the executable whose SHA-256 is
checked at startup. The existing run admission must still prove exclusive
host ownership, matching ports/processes/cgroup, resources and isolation from
foreign services. The launcher supplies no host or network lease.

Use a new experiment-owned root. Configurations must use absolute paths,
explicit managed data/cache directories and local DISK file services.
The actual directory inodes carry a required format xattr; unsupported file
systems cannot enable the feature. Declare every effective default file-service
and disk-cache root with repeated `--data-root` arguments if it is not explicit
in TOML. Service startup checks the complete effective set before opening roles. Prepare
before populating data directories. For a compatible closed D seed, copy it
only into the prepared run's declared data paths, preserving its independent
receipt; never let two arms share writable data.

```sh
python3 tools/data_cleaning_birth/guarded_launch.py prepare \
  --root /absolute/owned/run --binary /absolute/release/mo-service \
  --source FULL_COMMIT_SHA --native /absolute/verified/native-receipt.json \
  --launch /absolute/owned/run/launch.toml --run-id UNIQUE_RUN_ID --processors 8 \
  --data-root /absolute/owned/run/data/local \
  --data-root /absolute/owned/run/data/shared \
  --data-root /absolute/owned/run/data/shared-cache \
  --data-root /absolute/owned/run/data/etl \
  --data-root /absolute/owned/run/data/tmp
python3 tools/data_cleaning_birth/guarded_launch.py verify \
  --manifest /absolute/owned/run/birth-manifest.json \
  --binary /absolute/release/mo-service
python3 tools/data_cleaning_birth/guarded_launch.py run \
  --manifest /absolute/owned/run/birth-manifest.json \
  --binary /absolute/release/mo-service
```

`prepare` never overwrites a format identity. `run` verifies again under an
exclusive lock inherited across exec. The candidate checks its own executable,
manifest, command, configs and data binding before service startup; ordinary
builds containing this guard also reject a marked directory without admission.
The exact executable must be retained for crash/restart tests. A failed prepare
may leave a conservative marker; investigate it rather than deleting it to
bypass admission.

The marker and lock are an execution contract, not protection against a
same-user manual launch that bypasses the wrapper. Older binaries cannot be
made retroactively aware of a new format. If the exclusive owner cannot ensure
that all tools use this launcher and remain isolated, do not enable the feature.
Internal Go tests may construct a real self-hash/lock fixture; those are not
qualification of this service launcher or of Linux deployment.

Copying only a candidate data directory retains its required xattr. A controller
must not strip or rewrite that identity to admit another binary. Import of a
compatible, independently verified closed ordinary D seed must preserve the
fresh candidate directory markers and must pass `verify` after copying; this
launcher does not authorize importing arbitrary files or candidate-format seeds.
