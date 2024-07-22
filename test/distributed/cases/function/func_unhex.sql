SELECT unhex('616263');
SELECT unhex('68656c6c6f');
SELECT unhex('');
SELECT unhex(NULL);
SELECT unhex('invalid');
SELECT hex(unhex('616263'));
SELECT unhex(hex('abc'));