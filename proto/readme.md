 ```
 ____________
< Cerealize! >
 ------------
        \   ^__^
         \  (oo)\_______
            (__)\       )\/\
                ||----w |
                ||     ||
```

# Serializations

This directory contains protocol buffers schema for plan serializations.
Query plan may need serialization for several reasons, we may need
to ship plan to other nodes, save for plan caching, and to communicate
with external functions.

To change/update the protocol buffer schema, run make, and check in the
generated go file.

