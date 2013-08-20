(*
  Comprehensive tests for OCI*ML

  Requires setup as per ociml_test.sql

  14-AUG-2013 Gaius Initial version
*)

open Ociml
open Unix
open Printf
open Report
open Ociml_utils
open Testdata

let exitcode = ref 0
let test_dt_list = rand_dt_list () (* as we will need this later *)

(* The tests, all of type () -> test_status *)

let test_connect_to_db () = 
  try
    let lda = oralogon "ociml_test/ociml_test" in 
    oralogoff lda;
    Pass
  with
    Oci_exception (e_code, e_desc) -> Fail e_desc

let test_simple_select () = 
  try
    let lda = oralogon "ociml_test/ociml_test" in
    let sth = oraopen lda in
    orasql sth "select * from dual";
    let result = (orafetch sth).(0) in
    match result with
    |Varchar "X" -> Pass
    |_ -> Fail (orastring result)
 with
    Oci_exception (e_code, e_desc) -> Fail e_desc

let test_setup_test_table () = 
  try
    let lda = oralogon "ociml_test/ociml_test" in
    let sth = oraopen lda in
    orasql sth (dt_list_to_table_sql "tab1" test_dt_list);
    (* now describe it and see if it's as we expect *)
    let t = oradesc lda "tab1" in
    let dt_list2 = Array.map (fun (col_name, col_type, col_size, is_nullable) -> col_type) t in
    match (test_dt_list = (Array.to_list dt_list2)) with
    |true -> Pass
    |false -> Fail "DB does not match spec!"
  with
    Oci_exception (e_code, e_desc) -> Fail e_desc

let test_transactions_commit auto_mode () =
  try
    let lda1 = oralogon "ociml_test/ociml_test" in
    let lda2 = oralogon "ociml_test/ociml_test" in
    let sth1 = oraopen lda1 in
    let sth2 = oraopen lda2 in
    let test_row = rand_row 1 test_dt_list in
    ignore(match auto_mode with 
    |true -> oraautocom lda1 true;
    |false -> ());
    orasql sth1 "truncate table tab1";
    oraparse sth1 ("insert into tab1 values (" ^ (get_bind_vars test_dt_list) ^ ")");
    orabindexec sth1 [test_row];
    ignore(match auto_mode with
    |false -> oracommit lda1;
    |true -> ());
    oraclose sth1;
    oralogoff lda1;
    orasql sth2 "select * from tab1";
    let rs = orafetch sth2 in
    match (rs === test_row) with
    |true -> Pass
    |false -> Fail (
      String.concat "," 
	(Array.to_list 
	   (Array.mapi (fun i x -> 
	     match (x = test_row.(i)) with
	     |true -> ""
	     |false -> Printf.sprintf "got %s, but wrote %s" (orastring x) (orastring test_row.(i))
	    )
	      rs
	   )
	)
    )
  with
    Oci_exception (e_code, e_desc) -> Fail e_desc

(* drop the table then attempt to select from it. We should get ORA-942 from the SELECT *)
let test_drop_test_table () = 
  try
    let lda = oralogon "ociml_test/ociml_test" in
    let sth = oraopen lda in
    orasql sth "drop table tab1";
    orasql sth "select * from tab1";
    Fail "DROP TABLE did not work!"
  with
    Oci_exception (e_code, e_desc) -> match e_code with
    |942 -> Pass
    |_ -> Fail e_desc

let test_bulk_insert_performance rows batchsize () = 
  let dataset = rand_big_dataset test_dt_list rows in
  let lda = oralogon "ociml_test/ociml_test" in
  let sth = oraopen lda in
  orasql sth "truncate table tab1";
  oraparse sth ("insert into tab1 values (" ^ (get_bind_vars test_dt_list) ^ ")");
  let t1 = gettimeofday () in
  for i = 0 to (rows / batchsize) - 1 do
    orabindexec sth (sublist (i * batchsize) (((i + 1) * batchsize) - 1)  dataset)
  done;
  oracommit lda; (* commit at the end as we are testing OCI*ML performance, not Oracle *)
  let t2 = gettimeofday () -. t1 in
  oralogoff lda;
  Time (t2, float_of_int rows /. t2)


let test_prefetch_performance batchsize () =
  let lda = oralogon "ociml_test/ociml_test" in
  let sth = oraopen lda in
  let t1 = gettimeofday() in
  oraprefetch sth batchsize;
  orasql sth "select * from tab1";
  let rs = orafetchall sth in
  let t2 = gettimeofday () -. t1 in
  oralogoff lda;
  Time (t2, float_of_int (List.length rs) /. t2)

(* test that we can insert a row, issue a rollback, and that row isn't there anymore *)
let test_transactions_rollback () =
  try
    let lda = oralogon "ociml_test/ociml_test" in
    let sth = oraopen lda in
    let test_row = rand_row 1 test_dt_list in
    orasql sth "truncate table tab1";
    oraparse sth ("insert into tab1 values (" ^ (get_bind_vars test_dt_list) ^ ")");
    orabindexec sth [test_row];
    orasql sth "select count(1) from tab1";
    let count1 = (orafetch sth).(0) in 
    oraroll lda;
    orasql sth "select count(1) from tab1";
    let count2 = (orafetch sth).(0) in 
    oralogoff lda;
    match ((count1 = Integer 1) & (count2 = Integer 0)) with
    |true -> Pass
    |false -> Fail "Count not rollback statement!"
  with
   Oci_exception (e_code, e_desc) -> Fail e_desc

let test_bind_by_name_and_pos () = 
  try
    let lda = oralogon "ociml_test/ociml_test" in
    let sth = oraopen lda in
    orasql sth "truncate table tab1";

    oraparse sth ("insert into tab1 values (" ^ (get_bind_vars test_dt_list) ^ ")");
    let test_row1 = rand_row 99 test_dt_list in
    (* first test bind by position *)
    Array.iteri (fun i x -> (orabind sth (Pos (i+1)) x)) test_row1;
    oraexec sth;
    oracommit lda;
    oraparse sth "select * from tab1 where col0=:1";
    orabind sth (Pos 1) (Integer 99);
    oraexec sth;
    let rs1 = orafetch sth in

    let sql_text = ("insert into tab1 values (" ^ (get_named_bind_vars test_dt_list) ^ ")") in
    oraparse sth sql_text;
    let test_row2 = rand_row 1234 test_dt_list in
    (* second test bind by name *)
    Array.iteri (fun i x -> (
      let col_name = (sprintf "COL%d" i) in
      orabind sth (Name col_name) x)
    ) test_row2;
    oraexec sth;
    oracommit lda;
    oraparse sth "select * from tab1 where col0=:mycolumn";
    orabind sth (Name "mycolumn") (Integer 1234);
    oraexec sth;
    let rs2 = orafetch sth in

    match (rs1 === test_row1) & (rs2 === test_row2) with
    |true -> Pass
    |false -> Fail "Could not get row"
  with
    Oci_exception (e_code, e_desc) -> Fail e_desc


let test_autocommit () = 
  test_transactions_commit true ()

let test_aq () = 
  let lda = oralogon "ociml_test/ociml_test" in
  let test_msg = rand_aq_msg () in
  oraenqueue lda "message_queue" "message_t" test_msg;
  oracommit lda;
  let test_deq = oradequeue lda "message_queue" "message_t" [|Integer 0; Varchar ""|] in
  oracommit lda; (* need to commit to actually remove the message from the queue *)
  match (test_msg === test_deq) with
  |true -> Pass
  |false -> Fail "Messages do not match"
  
let test_aq_raw () =
  let lda = oralogon "ociml_test/ociml_test" in
  let cat = slurp_file "lynx.jpg" in
  oraenqueue lda "image_queue" "RAW" [|Binary cat|];
  oracommit lda;
  let test_deq = oradequeue lda "image_queue" "RAW" [|Binary ""|] in
  oracommit lda;
  match (cat = (match test_deq.(0) with |Binary x -> x|_ -> "" ))  with
  |true -> Pass
  |false -> Fail "Messages do not match"

let test_returning () = Todo
let test_ref_cursors () = Todo
    

(* list of tests to run *)

let functional_tests = [
  (test_connect_to_db, "oralogon", "Connection to database");
  (test_simple_select, "oraopen, orasql, orafetch", "SELECT * FROM DUAL;");
  (test_setup_test_table, "oradesc", "Setup test table");
  (test_transactions_commit false, "oraparse, orabindexec, oracommit", "Test COMMIT and SELECT from another session");
  (test_autocommit, "oraautocom", "Test autocommit mode");
  (test_transactions_rollback, "oraroll", "Test ROLLBACK"); 
  (test_bind_by_name_and_pos, "oraparse, orabind, oraexec", "Test binding by Name and by Pos");
  (test_aq, "oraenqueue, oradequeue", "Test AQ");
  (test_aq_raw, "oraenqueue, oradequeue", "Test AQ (Raw, requires lynx.jpg)");
  (test_returning, "orabindout", "Test the RETURNING/stored procedure syntax");
  (test_ref_cursors, "orabindout, orafetch", "Test cursor variables (REF CURSOR)");
  (test_drop_test_table, "", "Drop test table") ;
]

let performance_tests = [ 
  ((test_bulk_insert_performance 10000 1), "Bulk insert performance: 10000 rows, 1 row per batch");
  ((test_bulk_insert_performance 10000 10), "Bulk insert performance: 10000 rows, 10 rows per batch");
  ((test_bulk_insert_performance 10000 100), "Bulk insert performance: 10000 rows, 100 rows per batch");
  ((test_bulk_insert_performance 10000 1000), "Bulk insert performance: 10000 rows, 1000 rows per batch");
  ((test_bulk_insert_performance 10000 10000), "Bulk insert performance: 10000 rows, 10000 rows per batch");
   ((test_prefetch_performance 1), "Testing prefetch 1 row per fetch");
  ((test_prefetch_performance 10), "Testing prefetch 10 rows per fetch");
]

let () =
  print_endline "*** Running functional tests ***";
  oradebug false;
  let r = new report [|"Test"; "Uses"; "Result"|] in
  let run_test (f, uses, msg)  =
    r#add_row [|msg; uses; (
      match f () with
      |Pass -> "Pass"
      |Fail x -> 
	begin
	  exitcode := 1;
	  ("Fail (" ^ x ^ ")") 
	end
      |Todo -> "Test not implemented yet") |]
  in
  List.iter run_test functional_tests;
  r#print_report ();
  print_endline "*** Functional test suite complete ***";
  print_newline ()


let () =
  print_endline "*** Running performance tests ***";
  oradebug false;
  test_setup_test_table ();
  let r = new report [|"Test"; "Total time (s)"; "Rows/sec"|] in
  let run_test (f, msg)  =
      match f () with
      |Time (t, rs) -> r#add_row [|msg; (sprintf "%.2f" t); (sprintf "%.2f" rs)|]
  in
  List.iter run_test performance_tests;
  test_drop_test_table ();
  r#print_report ();
  print_endline "*** Performance test suite complete ***" 



let () =
  exit(!exitcode);
    
(* EOF *)
