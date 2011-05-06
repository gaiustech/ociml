(* Oracle API for OCaml vaguely based on OraTcl *)

open Unix
open Printf
open Scanf
open Oci_wrapper
open Log_message

(* give each connection a label for the purpose of logging *)
type meta_handle = int * oci_handles
let handle_counter = ref 0

(* public interface - open Oci_wrapper for direct access to the low-level functions *)
module type OCIML =
sig
  val oralogon:  string -> meta_handle
  val oralogoff: meta_handle -> unit
  val oracommit: meta_handle -> unit
  val oraroll:   meta_handle -> unit
end

(* Constants from oci.h *)
let oci_attr_username           =  22
let oci_attr_password           =  23 
let oci_attr_client_identifier  = 278 
let oci_attr_client_info        = 368 
let oci_attr_module             = 366 

let debug_on = true
let debug msg = match debug_on with |true -> log_message msg |false -> ()

(* Thrown if oci_env_create fails (which should never happen, and if it does we can't recover from it) *)
exception Ociml_exception of (int * string) 
let _ = Callback.register_exception "Ociml_exception" (Ociml_exception (-20000, "User defined error"))
  
(* do this just once at the start *)
let global_env = oci_env_create ()

(* commit work outstanding on this connection *)
let oracommit lda = 
  let (c, h) = lda in
  oci_commit h;
  debug (sprintf "connection id %d committed transaction" c);
  ()

(* rollback uncommitted work on this connection *)
let oraroll lda = 
  let (c, h) = lda in
  oci_rollback h;
  debug (sprintf "connection id %d rolled back transaction" c);
  ()


(* convert format "user/pass@db" or "user/pass" *)
let parse_connect_string c = sscanf c "%s@/%s@@%s"(fun u p d -> (u, p, d))

let oralogon connstr = 
  let h = oci_alloc_handles global_env in
  let (username, password, database) = parse_connect_string connstr in 
  oci_server_attach h database;
  oci_sess_set_attr h oci_attr_username username;
  oci_sess_set_attr h oci_attr_password password;
  oci_session_begin h;
  handle_counter := (!handle_counter + 1);
  let c = !handle_counter in 
  debug (sprintf "connection id %d logged into Oracle as %s@%s" c username database);
  (c, h)

let oralogoff lda =
  let (c, h) = lda in 
  oci_session_end h;
  oci_server_detach h;
  oci_free_handles h;
  debug (sprintf "connection id %d disconnected from Oracle" c);
  ()

(* End of file *)
