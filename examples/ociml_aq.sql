-- AQ test tables for OCI*ML
--
-- Need AQ_ADMINISTRATOR_ROLE and EXECUTE on DBMS_AQ for this to work
--
-- typed queue

create type message_t as object (
       message_id     integer,
       message_text   varchar2(80));

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

-- end of file 