parallel(2)

# on MacOS, run `sudo ifconfig lo0 alias 192.168.168.168` to add an ip to the loopback interface
#listen_host("192.168.168.168")
#listen_host("localhost")

port_range(40000, 50000)

timeout_report_threshold(1)

execute_timeout(15 * minute)

enable_cpu_profile(True)

http_server_addr("localhost:8889")

use_dummy_interface(False)
