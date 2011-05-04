(* Oracle API for OCaml vaguely based on OraTcl *)

open Oci_wrapper
open Unix
open Printf

(* Constants from oci.h *)
let oci_attr_username           =  22
let oci_attr_password           =  23 
let oci_attr_client_identifier  = 278 
let oci_attr_client_info        = 368 
let oci_attr_module             = 366 


(* Thrown if oci_env_create fails (which should never happen, and if it does we can't recover from it) *)
exception Ociml_exception of (int * string) 
let _ = Callback.register_exception "Ociml_exception" (Ociml_exception (-20000, "User defined error"))
  
let () = 
  oci_initialize ()

let () =     
    try
      let e = oci_env_create () in
      let h = oci_alloc_handles e in
      oci_server_attach h "XE";
      oci_sess_set_attr h oci_attr_username "gaius";
      oci_sess_set_attr h oci_attr_password "abc123";
      oci_session_begin h;
      oci_set_module e h (sprintf "%s/OCI*ML" Sys.executable_name);
      sleep 150
    with 
	Ociml_exception e -> 
	  match e with (e_code, e_desc) -> prerr_endline (sprintf "OCI*ML: %d: %s " e_code e_desc)
	  
(* End of file *)
