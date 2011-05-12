(* 
   Demo/test harness for OCI*ML library - requires table from ociml_test.sql 

   See accompanying file README and the manual for OraTcl 
   http://oratcl.sourceforge.net/manpage.html All supported functions are used
   in this file (ociml_sample.ml). 
*)

open Ociml
open Unix
open Report

let () =
  try 
    (* set debugging mode on - will go to stderr *)
    oradebug true;

    (* set the preferred value of NULL to an empty string *)
    oranullval (Varchar "");

    (* connect to database - edit with your own username and password as appropriate *)
    let lda = oralogon Sys.argv.(1) in

    (* open a new cursor/statement handle *)
    let sth = oraopen lda in

    (* execute one SQL statement - note all these functions can easily be 
       curried e.g let do_sql = (orasql sth) in do_sql "..." 
    *)
    orasql sth "delete from ociml_test";

    (* parse a SQL statement with bind variables *)
    oraparse sth "insert into ociml_test values (:myint, :mydate, :mystring, :myfloat)";

    (* set values for these variables by position *)
    orabind sth (Pos 1) (Integer 1);                      (* OCaml type int *)
    orabind sth (Pos 2) (Datetime (localtime (time ()))); (* OCaml type Unix.tm - down to seconds only, not millis *)
    orabind sth (Pos 3) (Varchar "hello!");               (* OCaml type string *)
    orabind sth (Pos 4) (Number 3.142);                   (* OCaml type float *)

    (* execute that transaction *)
    oraexec sth;

    (* commit that transaction *)
    oracommit lda;

    (* now set autocommit mode on *)
    oraautocom true;

    (* set values for these variables by name - note colon added automagically if missing *)
    orabind sth (Name "myint")    (Integer 2);
    orabind sth (Name ":mydate")  (Datetime (localtime (time ())));
    orabind sth (Name "mystring") (Varchar "goodbye!");
    orabind sth (Name ":myfloat") (Number 2.718);
    oraexec sth;
    
    (* Bind an array of values and execute - still in autocommit mode *)
    orabindexec sth [|(Integer 3); (Datetime (localtime (time ()))); (Varchar "hello again..."); (Number 1.41)|];

    (* describe the table - comes as array of name and type tuples - using my generic report formatter *)
    let decode_bool x = 
      match x with
	|true  -> "YES"
	|false -> "NO" in
    let decode_col_type x =
      match x with
	|2  (* oci_sqlt_num *)    -> "NUMBER"
	|12 (* oci_sqlt_dat *)    -> "DATE"
	|1  (* oci_sqlt_chr *)    -> "VARCHAR2"
	|_  (* something else! *) -> string_of_int x in
    let tabname = "ociml_test" in 
    let cols = oradesc lda tabname in
    let r = new report [|"Column name"; "Data type"; "Size"; "Is integer"; "NULL allowed"|] in
    Array.iter (fun (col_name, col_type, col_size, is_integer, is_nullable) -> 
    r#add_row [|col_name; (decode_col_type col_type); (string_of_int col_size); (decode_bool is_integer); (decode_bool is_nullable)|]) cols;
    r#print_report ();

    (* close the cursor *)
    oraclose sth;

    (* disconnect from the server *)
    oralogoff lda;

  with 
      (* Errors emitted by the underling OCI library - e_code will be set to familiar ORA numbers *)
      Oci_exception (e_code, e_desc) -> prerr_endline (Printf.sprintf "ociml_sample: %s " e_desc)

(* end of file *)
