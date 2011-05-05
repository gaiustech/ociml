(* Demo/test harness for OCI*ML library *)

open Ociml
open Unix

let () = 
  let lda = oralogon "guy" "abc123" "blame" in
  let lda2 = oralogon "guy" "abc123" "blame" in
  sleep 10

(* end of file *)
