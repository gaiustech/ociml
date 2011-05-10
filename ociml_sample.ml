(* Demo/test harness for OCI*ML library *)

open Ociml
open Unix
open Printf

let () =
  try 
    (* set debugging mode on *)
    oradebug := true;

    (* connect to database *)
    let lda = oralogon "guy/abc123" in

    (* open a new cursor/statement handle *)
    let sth = oraopen lda in

    (* execute one SQL statement *)
    orasql sth "delete from ociml_test";

    (* parse a SQL statement with bind variables *)
    oraparse sth "insert into ociml_test values (:myint, :mydate, :mystring, :myfloat)";

    (* set values for these variables by position *)
    orabind sth (Pos 1) (Integer 1);
    orabind sth (Pos 2) (Datetime (localtime (time ())));
    orabind sth (Pos 3) (Varchar "hello!");
    orabind sth (Pos 4) (Number 3.142);

    (* execute that transaction *)
    oraexec sth;

    (* commit that transaction *)
    oracommit lda;

    (* now set autocommit mode on *)
    oraautocom := true;

    (* set values for these variables by name - colon added if missing *)
    orabind sth (Name "myint") (Integer 2);
    orabind sth (Name ":mydate") (Datetime (localtime (time ())));
    orabind sth (Name "mystring") (Varchar "goodbye!");
    orabind sth (Name ":myfloat") (Number 2.718);
    oraexec sth;

    oraclose sth;
    oralogoff lda;

  with 
      Oci_exception e -> 
	match e with (e_code, e_desc) -> prerr_endline (sprintf "ociml_shell: %s " e_desc)

(* end of file *)
