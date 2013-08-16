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

let test_transactions () = Todo

(* drop the table then attempt to select from it. We should get ORA-942 from the SELECT *)
let test_drop_test_table () = 
  try
    let lda = oralogon "ociml_test/ociml_test" in
    let sth = oraopen lda in
    orasql sth "drop table tab1";
    orasql sth "select * from tab1";
    Pass
  with
    Oci_exception (e_code, e_desc) -> match e_code with
    |942 -> Pass
    |_ -> Fail e_desc

let get_bind_vars xs = 
  String.concat "," (Array.to_list (Array.mapi (fun pos a -> ":" ^ string_of_int (pos + 1)) (Array.of_list xs)))

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

(* list of tests to run *)

let functional_tests = [
  (test_connect_to_db, "oralogon", "Connection to database");
  (test_simple_select, "oraopen, orasql, orafetch", "SELECT * FROM DUAL;");
  (test_setup_test_table, "oradesc", "Setup test table");
  (test_transactions, "oracommit, oraroll", "Test tranactions");
  (test_drop_test_table, "", "Drop test table");
]

let performance_tests = [ 
  ((test_bulk_insert_performance 10000 1), "Bulk insert performance: 10000 rows, 1 row per batch");
  ((test_bulk_insert_performance 10000 10), "Bulk insert performance: 10000 rows, 10 rows per batch");
  ((test_bulk_insert_performance 10000 100), "Bulk insert performance: 10000 rows, 100 rows per batch");
  ((test_bulk_insert_performance 10000 1000), "Bulk insert performance: 10000 rows, 1000 rows per batch");
  ((test_bulk_insert_performance 10000 10000), "Bulk insert performance: 10000 rows, 10000 rows per batch");
  (* ((test_prefetch_performance 1), "Testing prefetch 1 row per fetch");
  ((test_prefetch_performance 10), "Testing prefetch 10 rows per fetch");
			  *)]

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
