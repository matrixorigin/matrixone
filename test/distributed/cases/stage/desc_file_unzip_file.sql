-- desc_file lists the name, size and directory flag of a file, of the entries of a directory, or
-- of the files matching a pattern; unzip_file returns the first file of a zip archive (#21825)
create stage zs URL='file:///$resources/load_data/zipzstd';
select substring_index(file, '/', -1) name, size, is_dir from desc_file('stage://zs/') d order by name;
select substring_index(file, '/', -1) name, size, is_dir from desc_file('stage://zs/*.zip') d order by name;
select substring_index(file, '/', -1) name, size, is_dir from desc_file('stage://zs/two.zip') d;
select substring_index(file, '/', -1) name from desc_file('stage://zs/mix/*') d order by name;
select count(*) from desc_file('stage://zs/missing') d;
select count(*) from desc_file(null) d;
select cast(unzip_file(cast('stage://zs/two.zip' as datalink)) as char) c;
select cast(unzip_file(cast('stage://zs/stored.zip' as datalink)) as char) c;
select length(unzip_file(cast('stage://zs/mix/m_2.csv.zip' as datalink))) n;
select cast(unzip_file('$resources/load_data/zipzstd/two.zip') as char) c;
select unzip_file(cast('stage://zs/rows_zst.csv.zst' as datalink));
select unzip_file(null);
drop stage zs;
