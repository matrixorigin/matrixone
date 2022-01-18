# number of parallel test cases running in `run` command
parallel(2)

# host address for cube to listen on
# this setting overwrites the use_dummy_interface setting
#listen_host("192.168.168.168")
#listen_host("localhost")

# port range for cube to use
port_range(40000, 50000)

# threshold for timeout reporting
timeout_report_threshold(1)

# timeout of single test case
execute_timeout(15 * minute)

# enable cpu profile for whole program
enable_cpu_profile(False)

# address for http server, mainly for net/http/pprof
#http_server_addr("localhost:8889")

# use dummy network interface to isolate, linux only
use_dummy_interface(False)

