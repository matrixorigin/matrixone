# number of parallel test cases running in `run` command
parallel(4)

# port range for cube to use
port_range(50000, 55000)

# threshold for timeout reporting
timeout_report_threshold(10)

# during this period, retry a get/set action if timeout occur
# after a node restart, timeouts are highly possible
retry_timeout(second * 15)

# timeout of single test case
execute_timeout(15 * minute)

# enable cpu profile for whole program
enable_cpu_profile(False)

# address for http server, mainly for net/http/pprof
#http_server_addr("localhost:8889")

# enable runtime/trace. trace files will be written to testdata/[uuid].runtiime.trace
enable_runtime_trace(True)

# network model
# localhost: use localhost
# dummy: use linux dummy interface
# tun: use TUN interface
#def set_network_model():
#    if os == 'linux':
#        network_model("tun")
#    else:
#        network_model("localhost")
#set_network_model()
network_model("dummy")

# temp dir model
# os: use os.TempDir()
# fuse: use in-memory fuse fs
# 9p: use 9p fs
#def set_temp_dir_mode():
#    if os == 'linux':
#        temp_dir_model("9p")
#    else:
#        temp_dir_model('os')
#set_temp_dir_mode()
temp_dir_model('os')

# enable fgprof github.com/felixge/fgprof
enable_fg_profile(False)

# enable 9p debugging
debug_9p(False)

