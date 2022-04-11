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

This directory contains flatbuffer schema for plan serializations.
Query plan may need serialization for several reasons, we may need
to ship plan to other nodes, save for plan caching, and to communicate
with external functions.

Used flatbuffer instead of protobuffer because some of the use 
cases actually care about performance.

To change/update the flatbuffer schema, run make, and check in the
generated go file.

