(* 
   quick query - simple example for ocimlsh

   see http://gaiustech.wordpress.com/2011/05/14/ocaml-as-a-sqlplus-replacement/

   $ ocimlsh 

   # #use "qq.ml";;
   # let lda = oralogon "gaius/abc123";;
   # let sth = oraopen lda;;
   # qq sth "select * from ociml_test";;
*)

open Ociml
open Report

let qq sth sqltext =
  orasql sth sqltext;
  let r = new report (Array.map orastring (oracols sth)) in
  begin
    try
      while true do
	let row = Array.map orastring (orafetch sth) in
	r#add_row row;
      done;
    with |Not_found -> ();
  end;
  r#print_report ();

(* end of file *)
