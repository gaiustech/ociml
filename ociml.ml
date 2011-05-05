(* Oracle API for OCaml vaguely based on OraTcl *)

open Unix
open Printf
open Oci_wrapper
open Log_message

(* Constants from oci.h *)
let oci_attr_username           =  22
let oci_attr_password           =  23 
let oci_attr_client_identifier  = 278 
let oci_attr_client_info        = 368 
let oci_attr_module             = 366 

let verbose = true
let debug msg = match verbose with |true -> log_message msg |false -> ()

(* Thrown if oci_env_create fails (which should never happen, and if it does we can't recover from it) *)
exception Ociml_exception of (int * string) 
let _ = Callback.register_exception "Ociml_exception" (Ociml_exception (-20000, "User defined error"))
  
(*let () = 
  oci_initialize ()*)

let global_env = oci_env_create ()

let oralogon username password database = 
  let h = oci_alloc_handles global_env in
  oci_server_attach h database;
  oci_sess_set_attr h oci_attr_username username;
  oci_sess_set_attr h oci_attr_password password;
  oci_session_begin h;
  debug (sprintf "logged into Oracle as %s@%s" username database);
  h

let () =     
    try
      let e = oci_env_create () in
      let h = oci_alloc_handles e in
      oci_server_attach h "BLAME";
      oci_sess_set_attr h oci_attr_username "guy";
      oci_sess_set_attr h oci_attr_password "abc123";
      oci_session_begin h;
      oci_set_module e h (sprintf "OCI*ML:%s" Sys.executable_name); (* just the default *)
      sleep 15;
      oci_session_end h;
      oci_server_detach  h;
      oci_free_handles e h;
      (* oci_terminate () *)
    with 
	Ociml_exception e -> 
	  match e with (e_code, e_desc) -> prerr_endline (sprintf "OCI*ML: %s " e_desc)
	  
(* End of file *)
