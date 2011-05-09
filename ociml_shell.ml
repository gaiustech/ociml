(* Demo/test harness for OCI*ML library *)

open Ociml
open Unix
open Printf

let () =
  try 
    oradebug := true;
    let lda = oralogon "guy/abc123" in
    let sth = oraopen lda in
    oraparse sth "insert into t1 values (:1, :2)";
    orabind sth (Pos 1) (Datetime (localtime (time ())));
    orabind sth (Pos 2) (Varchar "hello!");
    oraexec sth;
    oracommit lda;
    oraclose sth;
    oralogoff lda;

  with 
      Oci_exception e -> 
	match e with (e_code, e_desc) -> prerr_endline (sprintf "ociml_shell: %s " e_desc)

(* end of file *)
