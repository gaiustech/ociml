(* Oracle API for OCaml based on OraTcl *)

open Oci_wrapper

(* Thrown if oci_env_create fails (which should never happen, and if it does we can't recover from it) *)
exception Oci_env_fatal_exception
let _ = Callback.register_exception "Oci_env_fatal_exception" Oci_env_fatal_exception
  
let () = 
  oci_initialize ()

let () =     
    try
      let e = oci_env_create () in
      print_endline "OCI environment created!"
    with 
	Oci_env_fatal_exception -> prerr_endline "Cannot create an OCI environment (check ORACLE_HOME?)"
	  
(* End of file *)
