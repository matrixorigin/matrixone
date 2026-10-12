-- All public vector Base64 decoders report malformed input as user input errors.
select vecf32_from_base64('!!!');
select vecf64_from_base64('!!!');
select vecf16_from_base64('!!!');
select vecbf16_from_base64('!!!');
select vecint8_from_base64('!!!');
select vecuint8_from_base64('!!!');

-- One decoded byte is valid Base64, but is not a whole floating element.
select vecf32_from_base64('AA==');
select vecf64_from_base64('AA==');
select vecf16_from_base64('AA==');
select vecbf16_from_base64('AA==');

-- One byte is a complete UINT8 element; a failed query must not poison the session.
select vecuint8_from_base64('AA==');
