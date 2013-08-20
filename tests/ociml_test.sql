-- setup script for OCI*ML tests
--
-- to be run as
--
-- sqlplus / as sysdba @ociml_test.sql
--
-- This is a privileged user; it should only be setup in a dev system and/or locked when not in use.
--
-- 14-AUG-2013 Gaius Initial version

conn / as sysdba
drop user ociml_test cascade;

create user ociml_test identified by ociml_test default tablespace users quota 1g on users temporary tablespace temp;
grant connect, resource to ociml_test;
grant change notification to ociml_test;
grant aq_administrator_role to ociml_test;
grant execute on dbms_aq to ociml_test;
grant execute on dbms_aqadm to ociml_test;
grant alter session to ociml_test;

conn ociml_test/ociml_test

-- AQ test tables for OCI*ML
--
-- Need AQ_ADMINISTRATOR_ROLE and EXECUTE on DBMS_AQ for this to work
--
-- typed queue

create type message_t as object (
       message_id     integer,
       message_text   varchar2(80));
/

begin
	dbms_aqadm.create_queue_table (
		queue_table => 'tbl_message_queue',
		queue_payload_type => 'message_t');
				  
	dbms_aqadm.create_queue (
		queue_name => 'message_queue',
		queue_table => 'tbl_message_queue');

	dbms_aqadm.start_queue (
		queue_name => 'message_queue');
end;
/

create type int2_t as object (int1 integer, int2 integer);
/
begin
	dbms_aqadm.create_queue_table (
		queue_table => 'tbl_int2_queue',
		queue_payload_type => 'int2_t');
				  
	dbms_aqadm.create_queue (
		queue_name => 'int2_queue',
		queue_table => 'tbl_int2_queue');

	dbms_aqadm.start_queue (
		queue_name => 'int2_queue');
end;
/

		
-- raw queue

begin
	dbms_aqadm.create_queue_table (
		queue_table => 'tbl_image_queue',
		queue_payload_type => 'RAW');
				  
	dbms_aqadm.create_queue (
		queue_name => 'image_queue',
		queue_table => 'tbl_image_queue');

	dbms_aqadm.start_queue (
		queue_name => 'image_queue');
end;
/


create or replace package pkg_ref_cursor as
    type t_cursor is ref cursor;
    procedure pr_ref_cursor (p_refcur out t_cursor);
end pkg_ref_cursor;
/
 
-- this should be implicitly recompiled when used...
create table tab1(c1 number);
create or replace package body pkg_ref_cursor as
    procedure pr_ref_cursor (p_refcur out t_cursor) is
        v_cursor t_cursor;
    begin
        open v_cursor for select * from tab1;
        p_refcur := v_cursor;
    end pr_ref_cursor;
end pkg_ref_cursor;
/
drop table tab1;

quit
-- EOF