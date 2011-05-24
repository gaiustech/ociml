open Ociml

let lda = oralogon "gaius/abc123@xe";;
let sth = oraopen lda;;

orasql sth "alter session set events '10046 trace name context forever, level 12'";;
oradebug true;;
(*oraenqueue lda "int_q" "int_t" [|Integer 2|];;
oraenqueue lda "int_q" "int_t" [|Integer 3|];;
oraenqueue lda "int_q" "int_t" [|Integer 5|];;
oracommit lda;;
*)
(*
oraenqueue lda "int2_queue" "int2_t" [|Integer 2; Integer 7|];;
oraenqueue lda "int2_queue" "int2_t" [|Integer 3; Integer 13|];;
oraenqueue lda "int2_queue" "int2_t" [|Integer 5; Integer 99|];;
oracommit lda;;
*)

oraenqueue lda "message_queue" "message_t" [|Integer 15; Varchar "hello"|];;
oracommit lda;;

(* EOF *)
