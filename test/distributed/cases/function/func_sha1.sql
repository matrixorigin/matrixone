SELECT sha1('abc');
SELECT sha('abc');

SELECT sha1('');
SELECT sha('');

SELECT sha1(NULL);
SELECT sha(NULL);

-- sha1
select sha1('kfieli3453l5lj');
select sha1('  ');
select sha1('中文');
select sha1('0x45df');
select sha1('#$%^&*()!++={}|\~');
select sha1('flijeoijfoiejetjioejo349857789345789$%^&ljiofejiojaojfieoaio7934729749263589237592739284920983075jkncmnd,sfkjdsfsfflijeoijfoiejetjioejo349857789345789$%^&ljiofejiojaojfieoaio7934729749263589237592739284920983075jkncmnd,sfkjdsfsf');
select sha1(concat('oewo#$%',' 2335325'));
select sha1(4336);
select sha1(2008-09-09);
select sha1('2008-09-09');
select sha1(fsfsf);
-- sha
select sha('kfieli3453l5lj');
select sha('  ');
select sha('中文');
select sha('0x45df');
select sha('#$%^&*()!++={}|\~');
select sha('flijeoijfoiejetjioejo349857789345789$%^&ljiofejiojaojfieoaio7934729749263589237592739284920983075jkncmnd,sfkjdsfsfflijeoijfoiejetjioejo349857789345789$%^&ljiofejiojaojfieoaio7934729749263589237592739284920983075jkncmnd,sfkjdsfsf');
select sha(4336);
select sha(concat('oewo#$%',' 2335325'));
select sha(2008-09-09);
select sha('2008-09-09');
select sha(fsfsf);
