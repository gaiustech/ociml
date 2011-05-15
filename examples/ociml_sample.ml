(* 
   Demo/test harness for OCI*ML library - requires table from ociml_test.sql 

   See accompanying file README and the manual for OraTcl 
   http://oratcl.sourceforge.net/manpage.html 

   13-MAY-2011 Gaius Initial public version
*)

open Ociml
open Unix
open Printf
open Report (* a separate module for formatting tabular data *)
open Ociml_utils (* for decode_col_type *)
let () =
  try 
    (* set library debugging off - the default anyway - will go to stderr if enabled *)
    oradebug false;

    (* set the preferred value of NULL to something, defaults to 0 *)
    oranullval (Varchar "*NULL*");

    (* connect to the database *)
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
    orabind sth (Pos 3) (Varchar "PI");                   (* OCaml type string *)
    orabind sth (Pos 4) (Number 3.142);                   (* OCaml type float *)

    (* execute that transaction *)
    oraexec sth;

    (* commit that transaction *)
    oracommit lda;

    (* now set autocommit mode on *)
    oraautocom true;

    (* set values for these variables by name - note colon added automagically if missing *)
    orabind sth (Name "myint")    (Integer 2);
    orabind sth (Name ":mydate")  (Datetime (localtime 0.0));
    orabind sth (Name "mystring") (Varchar "e");
    orabind sth (Name ":myfloat") (Number 2.718);
    oraexec sth;
    
    (* Bind an array of values and execute - still in autocommit mode *)
    let my_constants = [
      [|(Integer 3); (Datetime (localtime (time ()))); (Varchar "Square root 2"); (Number 1.41)|];
      [|(Integer 4); (Datetime (localtime (time ()))); (Varchar "Speed of light"); (Number 300000000.)|];
      [|(Integer 5); (Datetime (localtime (time ()))); (Varchar "Acceleration of gravity"); (Number 9.8)|]
    ] in 
    List.iter (orabindexec sth) my_constants; 

    printf "Inserted 5 rows of data into the following table structure:\n\n";

    (* describe the table - comes as array of name and type tuples - using my generic report formatter *)
    let decode_bool x = 
      match x with
	|true  -> "YES"
	|false -> "NO" in
    let tabname = "ociml_test" in 
    let cols = oradesc lda tabname in
    let r = new report [|"Column name"; "Data type"; "Size"; "Is integer"; "NULL allowed"|] in
    Array.iter (fun (col_name, col_type, col_size, is_integer, is_nullable) -> 
    r#add_row [|col_name; 
		(decode_col_type col_type); 
		(string_of_int col_size); 
		(decode_bool is_integer); 
		(decode_bool is_nullable)|]) cols;
    r#print_report ();

    (* now run an actual query on the data *)
    oraparse sth "select * from ociml_test where constant_id=:1 or constant_id=:p2";
    orabind sth (Pos 1)     (Integer 1);
    orabind sth (Name "p2") (Integer 2);
    oraexec sth;

    printf "\nA query on that data:\n\n";

    let r = new report [|"Identity"; "Date entered"; "Description"; "Value"|]  in
    try
      while true do
	(* orastring converts any datatype to a string for display *)
	let row = Array.map orastring (orafetch sth) in
	r#add_row row;
      done;
    with 
      (* The exception is Not_found when reaching the end of the result set (like %NOTFOUND in PL/SQL) *)
      |Not_found -> ();
    r#print_report ();

    (* close the cursor *)
    oraclose sth;

    (* disconnect from the server *)
    oralogoff lda;

  with 
      (* Errors emitted by the underling OCI library - e_code will be set to familiar ORA numbers *)
      Oci_exception (e_code, e_desc) -> prerr_endline (sprintf "ociml_sample: %s " e_desc)

(* end of file *)
