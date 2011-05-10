-- test table for OCI*ML

begin
	execute immediate 'drop table ociml_test';
exception	
	when others then
	     null;
end;
/

-- one of each supported datatype
create table ociml_test (
       omlid   		integer primary key,
       omldate		date,
       omlstring	varchar2(80),
       omlfloat		number);

exit;

-- end of file