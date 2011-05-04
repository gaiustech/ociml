(* Oracle API for OCaml vaguely based on OraTcl *)

open Oci_wrapper
open Unix
open Printf

(* Thrown if oci_env_create fails (which should never happen, and if it does we can't recover from it) *)
exception Ociml_exception of (int * string) 
let _ = Callback.register_exception "Ociml_exception" (Ociml_exception (-20000, "User defined error"))
  
let () = 
  oci_initialize ()

let () =     
    try
      let e = oci_env_create () in
      let h = oci_alloc_handles e in
      oci_server_attach h "blamex"; 
      sleep 5
    with 
	Ociml_exception e -> 
	  match e with (e_code, e_desc) -> prerr_endline (sprintf "OCI*ML: %s " e_desc)
	  
(* End of file *)
