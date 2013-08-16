(* 
   Utility functions for generating and manipulating test datasets 

   We can generate a list of datatypes with rand_dt_list which will give a list 
   of between 8 and 16 Ociml datatypes. This list can be used to generate table
   DDL with dt_list_to_table_sql and a dataset suitable for use with orabindexec
   with rand_big_dataset, or just a single row with rand_row. Obviously those
   can also be used with a manually specific list of datatypes.
*)

open Ociml
open Ociml_utils
open Unix

type test_status = Pass|Todo|Fail of string|Time of float * float (* time and rows/time *)

let () = Random.self_init ()    

(* functions for generating random data *)
    
(* return a random datatype *)
let rand_dt () = match Random.int 4 with 
  |0 -> (Integer 0)
  |1 -> (Number 0.0)
  |2 -> (Varchar "")
  |_ -> (Datetime (localtime 0.0))
    
(* generate a random list of datatypes starting with an Integer for PK *)
let rand_dt_list () = 
  let rec rand_tab_helper xs n = match n with
    |0 -> xs
    |_ -> rand_tab_helper ((rand_dt ()) :: xs) (n - 1)
  in
  List.rev (rand_tab_helper [(Integer 0)] (7 + Random.int 8))
    
(* take a list of of datatypes and return some random data for all apart from the first *)
let rand_row n dt_list = 
  let rand_int () = (Integer (Random.int 1000000)) in
  let rand_number () = (Number (Random.float 1000000000.)) in
  let rand_date () = (Datetime (localtime (Random.float (time() *. 2.)))) in
  let rand_varchar () = 
    let rec rand_varchar_helper acc n = match n with
      |0 -> acc
      |_ -> rand_varchar_helper (acc ^ (Char.escaped (Char.chr (97 + (Random.int 26))))) (n - 1) in
    (Varchar (rand_varchar_helper "" (Random.int 2048))) in
  let rand_dt x = match x with
    |Integer _ -> rand_int ()
    |Number _ -> rand_number ()
    |Datetime _ -> rand_date ()
    |Varchar _ -> rand_varchar () in
  Array.of_list ((Integer n) :: (List.map rand_dt (List.tl dt_list)))
    
(* generate a big random dataset *)
let rand_big_dataset dt_list n = 
  let rec rand_big_dataset_helper acc n =
    match n with
    |0 -> acc
    |_ -> rand_big_dataset_helper ([(rand_row n dt_list)]::acc) (n - 1) in
  List.flatten (rand_big_dataset_helper [] n)
    
(* borrowed from Stack Overflow http://stackoverflow.com/q/2710233/447514 *)
let rec sublist b e l = 
  match l with
    [] -> failwith "sublist"
  | h :: t -> 
    let tail = if e=0 then [] else sublist (b-1) (e-1) t in
    if b>0 then tail else h :: tail
      
(* convert a list of datatypes into DDL *)
let dt_list_to_table_sql tabname dt_list = 
  let dt_to_sql dt = match dt with
    |Integer _ -> "INTEGER"
    |Number _ -> "NUMBER"
    |Datetime _ -> "DATE"
    |_ -> "VARCHAR2(2048)" in
  let rec dt_list_to_sql sql dt_list_len dt_list = match dt_list with
    |[] -> sql
    |x::xs -> let sql_line = "col" ^ (string_of_int (dt_list_len - List.length dt_list)) ^ " " ^ dt_to_sql x in
	      match sql with 
	      |"" -> dt_list_to_sql (sql_line ^ " PRIMARY KEY") dt_list_len xs
	      |_ -> dt_list_to_sql (String.concat "," [sql; sql_line]) dt_list_len xs in
  "CREATE TABLE " ^ tabname ^ " (\n" ^ (dt_list_to_sql "" (List.length dt_list) dt_list) ^ ")\n"
    
(* End of file *)
