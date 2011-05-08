(* Demo/test harness for OCI*ML library *)

open Ociml
open Unix
open Printf

let () =
  try 
    oradebug := true;
    let lda = oralogon "gaius/abc123" in
    let sth = oraopen lda in
    oraparse sth "insert into t2 values (:1)";
    orabind sth (Pos 1) (Varchar "hello");
    oraexec sth;
    oracommit lda;
    oraclose sth;
    oralogoff lda;

  with 
      Oci_exception e -> 
	match e with (e_code, e_desc) -> prerr_endline (sprintf "ociml_shell: %s " e_desc)

(* end of file *)
