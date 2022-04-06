# Error Handling

Rule is very simple, in the code, if an error can be handled
by caller, use comma ok.   If the error cannot be handled and
will result in query abort, just panic.

You should not recover any panics unless it is at a few well 
defined places.  It really should be just one place, but 
well, we have a few.

Our code should ALWAYS panic with a well defined moerr error.
This will make sure a meaningful error message pops up to user.
When generate an error, please look at the already defined 
error code.  If none fits, create your own error code.
It is arguably better to put error message format in the Error
class as well, maybe we will sometime later.

We should have prevented all runtime panics, but if it really
happens during runtime, we should convert the panic to an internal
error. *ANY internal error is a bug*.

There are also a few error code that are not considered error.
They are used to give user a meaningful info/warn message.  An
example will be truncation for varchar(N) columns.  NYI yet.

