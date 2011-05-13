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
       constant_id   	integer primary key,
       date_entered	date,
       constant_name	varchar2(80),
       const_value	number);

exit;

-- end of file