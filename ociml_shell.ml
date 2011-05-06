(* Demo/test harness for OCI*ML library *)

open Ociml
open Unix
open Printf

let () =
  try 
    let lda = oralogon "guy/abc123" in
    let lda2 = oralogon "guy/abc123@blame" in
    let sth = oraopen lda in
    let sth2 = oraopen lda in
    sleep 10;
    oracommit lda;
    oraroll lda2;
    oraclose sth2;
    oraclose sth;
    oralogoff lda;
    oralogoff lda2
  with 
      Ociml_exception e -> 
	match e with (e_code, e_desc) -> prerr_endline (sprintf "OCI*ML: %s " e_desc)

(* end of file *)
