# bpftrace

This directory contains a bunch of bpftrace scripts
for debugging mo.

## Installation

Only covers ubuntu 22.04 and WSL ubuntu.  Install bpftrace
using `sudo apt install bpftrace`.

You must install debug symbols to make bpftrace work.
Follow [install debug symbols](https://wiki.ubuntu.com/Debug%20Symbol%20Packages)
then `sudo apt install bpftrace-dbgsym`.

You also need to mount debugfs.  `sudo mount -t debugfs debugfs /sys/kernel/debug`

## Example

An example, `disttae_ins.bt` can track and print out stats of latency of 
disttae insert call.
```
sudo bpftrace ./disttae_ins.bat
```

Hit `^C` to quit and if everything works, it will print otu a histogram
of time spent in disttae insert.


