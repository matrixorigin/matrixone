-- test encode/decode function
SELECT encode('\xa7', 'hex');
SELECT decode('616263', 'hex');

SELECT encode('abc', 'hex'), decode('616263', 'hex');
SELECT encode('abc', 'base64'), decode('YWJj', 'base64');

SELECT decode('invalid', 'hex');
SELECT decode('invalid', 'base64');

SELECT encode('abc', 'fake');
SELECT decode('abc', 'fake');