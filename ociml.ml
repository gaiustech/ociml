(* Oracle API for OCaml vaguely based on OraTcl *)

open Unix
open Printf
open Scanf
open Oci_wrapper
open Log_message

(* data structure for use within the library bundling all the handles associated 
   with a connection with a unique identifier and some useful statistics *)
type meta_handle = {connection_id:int; 
		    mutable commits:int; 
		    mutable rollbacks:int; 
		    lda:oci_handles}
let handle_seq = ref 0

(* same with statements, counters for parses, binds and execs, and the parent 
   connection (as it is allocated from the global OCI environment) *)
type meta_statement = {statement_id:int; 
		       mutable parses:int;
		       mutable binds:int;
		       mutable execs:int;
		       parent_lda:meta_handle; 
		       sth:oci_statement}
let statement_seq = ref 0

(* public interface - open Oci_wrapper for direct access to the low-level 
   functions - should not be necessary for day-to-day work *)
module type OCIML =
sig
  val oralogon:  string -> meta_handle
  val oralogoff: meta_handle -> unit
  val oracommit: meta_handle -> unit
  val oraroll:   meta_handle -> unit
  val oraopen:   meta_handle -> meta_statement
  val oraclose:  meta_statement -> unit
  val oradebug:  bool
end

(* various constants from oci.h *)
let oci_attr_username           =  22
let oci_attr_password           =  23 
let oci_attr_client_identifier  = 278 
let oci_attr_client_info        = 368 
let oci_attr_module             = 366 

(* write a timestamped log message (log messages from the C code are tagged {C} 
   so anything else is from the ML *)
let oradebug = ref false
let debug msg = match !oradebug with |true -> log_message msg |false -> ()

(* for exceptions thrown back by the C code, I generally intend that OCI itself 
   does the bulk of the error checking *)
exception Ociml_exception of (int * string) 
let _ = Callback.register_exception "Ociml_exception" (Ociml_exception (-20000, "User defined error"))
  
(* do this just once at the start *)
let global_env = oci_env_create ()

let oraclose sth = 
  debug (sprintf "freeing statement id %d parent connection id %d" sth.statement_id sth.parent_lda.connection_id);
  oci_free_statement sth.sth

(* open a statement handle/cursor on a given connection - actually allocated 
   from the global env's memory pool as the OCI sample code seems to do it 
   that way *)
let oraopen lda =
  let s = oci_alloc_statement global_env in
  statement_seq := (!statement_seq + 1);
  let c = !statement_seq in
  debug (sprintf "allocated statement id %d parent connection id %d" c lda.connection_id);
  {statement_id=c; parses=0; binds=0; execs=0; parent_lda=lda; sth=s}

(* connect to Oracle, connstr in format "user/pass@db" or "user/pass" like OraTcl *)
let oralogon connstr = 
  let h = oci_alloc_handles global_env in
  let parse_connect_string c = sscanf c "%s@/%s@@%s"(fun u p d -> (u, p, d)) in
  let (username, password, database) = parse_connect_string connstr in 
  oci_server_attach h database;
  oci_sess_set_attr h oci_attr_username username;
  oci_sess_set_attr h oci_attr_password password;
  oci_session_begin h;
  handle_seq := (!handle_seq + 1);
  let c = !handle_seq in 
  debug (sprintf "connection id %d connected to Oracle as %s@%s" c username database);
  {connection_id=c; commits=0; rollbacks=0; lda=h}

(* Disconnect from Oracle and release the memory. Global env is still allocated *)
let oralogoff lda =
  oci_session_end lda.lda;
  oci_server_detach lda.lda;
  oci_free_handles lda.lda;
  debug (sprintf "connection id %d disconnected from Oracle" lda.connection_id);
  ()

(* commit work outstanding on this connection *)
let oracommit lda = 
  oci_commit lda.lda;
  lda.commits <- (lda.commits + 1);
  debug (sprintf "connection id %d committed transaction %d" lda.connection_id lda.commits);
  ()

(* rollback uncommitted work on this connection *)
let oraroll lda = 
  oci_rollback lda.lda;
  lda.rollbacks <- (lda.rollbacks + 1);
  debug (sprintf "connection id %d rolled back transaction, %d rollbacks" lda.connection_id lda.rollbacks);
  ()

(* End of file *)
