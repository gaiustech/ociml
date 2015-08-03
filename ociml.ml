(* Oracle API for OCaml vaguely based on OraTcl *)

open Unix
open Printf
open Scanf
open Log_message
open Ociml_utils

(* Fairly thin wrapper around low-level OCI functions, used to implement higher-level OCI*ML library *)

(* OCI handles that have to be tracked at the application level *)
type oci_env        (* global OCI environment *)
type oci_handles    (* C struct that bundles error, server, service context and session handles *)
type oci_statement  (* statement handle *)
type oci_bindhandle (* for binding in prepared statements *)
type oci_ptr        (* void* pointer so we can heap alloc for binding/defining *)

(* data structure for use within the library bundling all the handles associated 
   with a connection with a unique identifier and some useful statistics *)
type meta_handle = {connection_id:int; 
		    mutable commits:int; 
		    mutable rollbacks:int;
		    mutable lda_op_time:float;
		    mutable auto_commit:bool;
		    mutable deq_timeout:int;
		    lda:oci_handles}

(* variant enabling binding by position or by name *)
type bind_spec = Pos of int|Name of string

type nullable = Nullable|Not_nullable

(* define includes datatype for later fetching *)
type define_spec = {dtype:int; is_int:bool ; is_null:bool; ptr:oci_ptr}


(* Variant for the basic data types - datetime crosses back and forth as epoch, 
   and Oracle's NUMBER datatype of course can be either integer or floating 
   point but it doesn't make sense to make the OCaml layer deal only in floats *)
type col_value = Col_type of string * int * int * bool * bool
		 |Varchar of string
		 |Datetime of Unix.tm
		 |Integer of int
		 |Number of float
		 |Null
		 |RefCursor
		 |Statement of meta_statement
		 |Binary of string
and
(* same with statements, counters for parses, binds and execs, and the parent 
   connection (as it is allocated from the global OCI environment) *)
  meta_statement = {statement_id:int; 
		    mutable parses:int;
		    mutable binds:int;
		    mutable execs:int;
		    mutable sth_op_time:float;
		    mutable prefetch_rows:int;
		    mutable rows_affected:int;
		    mutable num_cols:int;
		    mutable sql_type:int;
		    mutable out_pending:bool;
		    mutable out_counter:int;
		    out_types:(bind_spec, col_value) Hashtbl.t;
		    bound_vals:(bind_spec, oci_bindhandle) Hashtbl.t;
		    defined_vals:(bind_spec, define_spec) Hashtbl.t;
		    oci_ptrs:(bind_spec, oci_ptr) Hashtbl.t;
		    ref_cursors:(bind_spec, oci_statement) Hashtbl.t;
		    parent_lda:meta_handle; 
		    sth:oci_statement}

let date_to_double t = fst (mktime t)

let decode_col_type x =
  match x with
    |2  (* oci_sqlt_num *)    -> "NUMBER"
    |12 (* oci_sqlt_dat *)    -> "DATE"
    |1  (* oci_sqlt_chr *)    -> "VARCHAR2"
    |_  (* something else! *) -> string_of_int x
	  	  
(* setup functions, in order in which they should be called - oci_connect.c *)
external oci_env_create: unit -> oci_env = "caml_oci_env_create"
external oci_alloc_handles: oci_env -> oci_handles = "caml_oci_alloc_handles"
external oci_server_attach: oci_handles -> string -> unit = "caml_oci_server_attach" (* takes db name *)
external oci_sess_set_attr: oci_handles -> int -> string -> unit = "caml_oci_sess_set_attr" 
external oci_session_begin: oci_handles -> unit = "caml_oci_session_begin" (* username and password set as attrs *)
external oci_set_module: oci_env -> oci_handles -> string -> unit = "caml_oci_set_module"

(* teardown functions - oci_connect.c *)
external oci_session_end: oci_handles -> unit = "caml_oci_session_end"
external oci_server_detach: oci_handles -> unit = "caml_oci_server_detach"
external oci_free_handles: oci_handles -> unit = "caml_oci_free_handles"
external oci_terminate: oci_env -> unit = "caml_oci_terminate" (* final cleanup *)
external oci_break: oci_handles -> unit = "caml_oci_break"

(* transaction control commit/rollback - oci_dml.c *)
external oci_commit: oci_handles -> unit = "caml_oci_commit"
external oci_rollback: oci_handles -> unit = "caml_oci_rollback"

(* statement handles - oci_dml.c *)
external oci_alloc_statement: oci_env -> oci_statement = "caml_oci_stmt_alloc"
external oci_free_statement: oci_statement -> unit = "caml_oci_stmt_free"

(* basic DML (enough for orasql) - oci_dml.c *)
external oci_statement_prepare: oci_handles -> oci_statement -> string -> int = "caml_oci_stmt_prepare"
external oci_statement_execute: oci_handles -> oci_statement -> bool -> bool -> unit = "caml_oci_stmt_execute" (* AUTOCOMMIT and DESCRIBE_ONLY *)

(* binding - oci_dml.c *)
external oci_alloc_bindhandle: unit -> oci_bindhandle = "caml_oci_alloc_bindhandle"
external oci_bind_by_pos: oci_handles -> oci_statement -> oci_bindhandle -> (int * int) -> col_value -> oci_ptr = "caml_oci_bind_by_pos" 
external oci_bind_date_by_pos: oci_handles -> oci_statement -> oci_bindhandle -> int -> float -> oci_ptr = "caml_oci_bind_date_by_pos"
external oci_bind_by_name: oci_handles -> oci_statement -> oci_bindhandle -> (string * int) -> col_value -> oci_ptr = "caml_oci_bind_by_name" 
external oci_bind_date_by_name: oci_handles -> oci_statement -> oci_bindhandle -> string -> float -> oci_ptr = "caml_oci_bind_date_by_name" 

(* fetching - oci_select.c *)
external oci_get_column_types: oci_handles -> oci_statement -> col_value array = "caml_oci_get_column_types"
external oci_define: oci_handles -> oci_statement -> int -> (int * bool * bool) -> int -> define_spec = "caml_oci_define"
external oci_fetch: oci_handles -> oci_statement -> unit = "caml_oci_fetch"
external oci_set_prefetch: oci_handles -> oci_statement -> int -> unit = "caml_oci_set_prefetch"
external oci_get_rows_affected: oci_handles -> oci_statement -> int = "caml_oci_get_rows_affected"

(* type conversions - oci_types.c *)
external oci_get_defined_string: oci_ptr -> string = "caml_oci_get_defined_string"
external oci_get_date_as_double: oci_ptr -> float = "caml_oci_get_date_as_double"
external oci_get_double: oci_handles -> oci_ptr -> float = "caml_oci_get_double"
external oci_get_int: oci_handles -> oci_ptr -> int = "caml_oci_get_int"

(* C heap memory functions - oci_common.c *)
external oci_alloc_c_mem: int -> oci_ptr = "caml_alloc_c_mem"
external oci_size_of_pointer: unit -> int = "caml_oci_size_of_pointer"
external oci_size_of_number: unit -> int = "caml_oci_size_of_number" (* Size of OCINumber *)
external oci_constant_assign: oci_ptr -> int -> int -> unit = "c_write_int_at_offset"
external oci_write_ptr_at_offset: oci_ptr -> int -> oci_ptr -> unit = "caml_write_ptr_at_offset"
external oci_read_ptr_at_offset: oci_ptr -> int -> bool -> oci_ptr = "caml_read_ptr_at_offset"
external oci_write_int_at_offset: oci_handles -> oci_ptr -> int -> int -> unit = "caml_oci_write_int_at_offset"
external oci_write_flt_at_offset: oci_handles -> oci_ptr -> int -> float -> unit = "caml_oci_write_flt_at_offset"
external oci_version: unit -> (int * int) = "caml_oci_version"

(* AQ functions - oci_aq.c *)
external oci_get_tdo_: oci_env -> oci_handles -> string -> oci_ptr = "caml_oci_get_tdo"
external oci_string_assign: oci_env -> oci_handles -> string -> oci_ptr = "caml_oci_string_assign_text"
external oci_aq_enqueue: oci_handles -> string -> oci_ptr -> oci_ptr -> oci_ptr -> unit = "caml_oci_aq_enqueue"
external oci_int_from_number: oci_handles -> oci_ptr -> int -> int = "caml_oci_int_from_number"
external oci_flt_from_number: oci_handles -> oci_ptr -> int -> float = "caml_oci_flt_from_number"
external oci_string_from_string: oci_env -> oci_ptr -> string = "caml_oci_string_from_string"
external oci_aq_dequeue: oci_env -> oci_handles -> string -> oci_ptr -> int -> oci_ptr = "caml_oci_aq_dequeue"
external oci_aq_enqueue_raw: oci_env -> oci_handles -> string -> oci_ptr -> string -> unit = "caml_oci_aq_enqueue_raw"
external oci_aq_dequeue_raw: oci_env -> oci_handles -> string -> oci_ptr -> int -> string = "caml_oci_aq_dequeue_raw"

(* Out variable functions - oci_out.c *)
external oci_bind_numeric_out_by_pos: oci_handles -> oci_statement -> oci_bindhandle -> int -> oci_ptr = "caml_oci_bind_numeric_out_by_pos"
external oci_get_int_from_context: oci_handles -> oci_ptr -> int -> int = "caml_oci_get_int_from_context"
external oci_get_float_from_context: oci_handles -> oci_ptr -> int -> float = "caml_oci_get_float_from_context"
external oci_bind_date_out_by_pos: oci_handles -> oci_statement -> oci_bindhandle -> int -> oci_ptr = "caml_oci_bind_date_out_by_pos"
external oci_get_date_from_context: oci_handles -> oci_ptr -> int -> float = "caml_oci_get_date_from_context"
external oci_bind_string_out_by_pos: oci_handles -> oci_statement -> oci_bindhandle -> int -> oci_ptr = "caml_oci_bind_string_out_by_pos"
external oci_get_string_from_context: oci_handles -> oci_ptr -> int -> string = "caml_oci_get_string_from_context"
external oci_get_pos_from_name: oci_handles -> oci_statement -> string -> int = "caml_oci_get_pos_from_name"
external oci_bind_ref_cursor: oci_handles -> oci_statement -> oci_bindhandle -> int -> oci_statement -> unit = "caml_oci_bind_ref_cursor"

(* bulk dml functions - oci_bulkdml.c *)
external oci_size_of_int: unit -> int = "caml_oci_get_size_of_int" (* native datatypes, not OCINumber *)
external oci_size_of_float: unit -> int = "caml_oci_get_size_of_float"
external oci_size_of_date: unit -> int = "caml_oci_get_size_of_date" (* this one is an OCIDate *)
external oci_write_nat_int_at_offset: oci_ptr -> int -> int -> unit = "caml_oci_write_nat_int_at_offset"
external oci_write_nat_flt_at_offset: oci_ptr -> int -> float -> unit = "caml_oci_write_nat_flt_at_offset"
external oci_write_chr_at_offset: oci_ptr -> int -> string -> unit = "caml_oci_write_chr_at_offset"
external oci_write_odt_at_offset: oci_ptr -> int -> float -> unit = "caml_oci_write_odt_at_offset" (* takes a double and converts it to an OCIDate *)
external oci_bind_bulk_int: oci_handles -> oci_statement -> oci_bindhandle -> oci_ptr -> int -> unit = "caml_oci_bind_bulk_int"
external oci_bind_bulk_flt: oci_handles -> oci_statement -> oci_bindhandle -> oci_ptr -> int -> unit = "caml_oci_bind_bulk_flt"
external oci_bind_bulk_chr: oci_handles -> oci_statement -> oci_bindhandle -> oci_ptr -> (int * int) -> unit = "caml_oci_bind_bulk_chr"
external oci_bind_bulk_odt: oci_handles -> oci_statement -> oci_bindhandle -> oci_ptr -> (int * int) -> unit = "caml_oci_bind_bulk_odt"
external oci_bulk_exec: oci_handles -> oci_statement -> int -> bool -> unit = "caml_oci_bulk_exec"

(* public interface *)
module type OCIML =
sig
  val oralogon:     string -> meta_handle
  val oralogoff:    meta_handle -> unit
  val oracommit:    meta_handle -> unit
  val oraroll:      meta_handle -> unit
  val oraopen:      meta_handle -> meta_statement
  val oraclose:     meta_statement -> unit
  val oraparse:     meta_statement -> string -> unit
  val oraexec:      meta_statement -> unit
  val orabind:      meta_statement -> bind_spec -> col_value -> unit
  val oradebug:     bool -> unit
  val orasql:       meta_statement -> string -> unit
  val oraautocom:   meta_handle -> unit
  val orabindexec:  meta_statement -> col_value array list -> unit
  val orastring:    col_value -> string
  val oradesc:      meta_handle -> string -> string array
  val oracols:      meta_statement -> string array
  val orafetch:     meta_statement -> col_value array
  val orafetchall:  meta_statement -> col_value array list
  val oranullval:   col_value -> unit
  val oraenqueue:   meta_handle -> string -> string -> col_value array -> unit
  val oradequeue:   meta_handle -> string -> string -> col_value array -> col_value array
  val oradeqtime:   meta_handle -> int -> unit
  val oraprefetch:  meta_statement -> int -> unit
  val oraprompt:    string
  val oraprefetch_default: int
  val oci_version:  unit -> (int * int)
  val oraldalist:   unit -> meta_handle list
  val orasthlist:   meta_handle -> meta_statement list
end

(* actual implementation *)
let handle_seq = ref 0 (* unique ids for handles *)
let statement_seq = ref 0 (* unique ids for statements *)

let open_connections = Hashtbl.create 10 (* currently opened handles *)
let oraldalist () = hash_vals open_connections

let open_statements  = Hashtbl.create 20 (* currently opened statement handles *)
let orasthlist lda = 
  Hashtbl.fold (
    fun (connection_id, _) v acc -> 
      if (lda.connection_id == connection_id) then
	v::acc
      else
	acc
  ) open_statements []

(* write a timestamped log message (log messages from the C code are tagged {C} 
   so anything else is from the ML. This can be set from the application.  *)
let internal_oradebug = ref false
let oradebug x = internal_oradebug := x; ()
let debug msg = match !internal_oradebug with |true -> log_message msg |false -> ()

(* autocommit mode - default false *)
let oraautocom lda x = lda.auto_commit <- x; ()

(* time to wait for a dequeue *)
let oradeqtime lda x = lda.deq_timeout <- x; ()

(* rows to prefetch - set at the level of a statement *)
let oraprefetch sth x = sth.prefetch_rows <- x; ()
let oraprefetch_default = ref 10

let oraprompt = ref "not connected > "

(* set this to what you want NULLs to be returned as, e.g. Integer 0 or Varchar "" or Datetime 0.0 even! *)
let internal_oranullval = ref @@ (Varchar "null")
let oranullval x =
  match x with
    |Null -> () (* ignore this request! *)
    |_ -> internal_oranullval := x; ()

(* for exceptions thrown back by the C code, I generally intend that OCI itself 
   does the bulk of the error checking *)
exception Oci_exception of (int * string) 
let _ = Callback.register_exception "Oci_exception" (Oci_exception (-20000, "User defined error"))
  
(* do this just once at the start - cleaned up by atexit in the C code *)
let global_env = oci_env_create ()

(* bind a value into a placeholder in a statement, can be either an offset of
   type integer (starting from 1) or the name of the placeholder e.g. :varname.
   Note that if you bind the same column by position and by name in subsequent
   calls you will have a small leak in the bind handle cache until the next parse*)
let rec orabind sth bs cv =
  (* if we have a bind handle for this bind spec, reuse it otherwise allocate a new one *)
  let bh = (try 
	      Hashtbl.find sth.bound_vals bs
    with Not_found -> let bh = oci_alloc_bindhandle () in
		      Hashtbl.add sth.bound_vals bs bh; 
		      bh ) in
  (* note that we are maintaining pointers to the bindhandle *and* the bound value, so they can be heap-allocated and GC'd later *)
  (match bs with
    |Pos p -> (match cv with
	|Datetime x -> (Hashtbl.replace sth.oci_ptrs bs (oci_bind_date_by_pos sth.parent_lda.lda sth.sth bh p (date_to_double x)))
	|Varchar _  -> (Hashtbl.replace sth.oci_ptrs bs (oci_bind_by_pos sth.parent_lda.lda sth.sth bh (p, oci_sqlt_str) cv))
	|Integer _  -> (Hashtbl.replace sth.oci_ptrs bs (oci_bind_by_pos sth.parent_lda.lda sth.sth bh (p, oci_sqlt_int) cv))
	|Number _   -> (Hashtbl.replace sth.oci_ptrs bs (oci_bind_by_pos sth.parent_lda.lda sth.sth bh (p, oci_sqlt_flt) cv))
	|_          -> orabind sth bs !internal_oranullval
    )
    |Name n -> ( 
      let n = match (String.sub n 0 1) with 
	|":" -> n
	|_   -> (sprintf ":%s" n) in
      match cv with
	|Datetime x -> (Hashtbl.replace sth.oci_ptrs bs (oci_bind_date_by_name sth.parent_lda.lda sth.sth bh n (date_to_double x)))
	|Varchar _  -> (Hashtbl.replace sth.oci_ptrs bs (oci_bind_by_name sth.parent_lda.lda sth.sth bh (n, oci_sqlt_str) cv))
	|Integer _  -> (Hashtbl.replace sth.oci_ptrs bs (oci_bind_by_name sth.parent_lda.lda sth.sth bh (n, oci_sqlt_int) cv))
	|Number _   -> (Hashtbl.replace sth.oci_ptrs bs (oci_bind_by_name sth.parent_lda.lda sth.sth bh (n, oci_sqlt_flt) cv))
	|_          -> orabind sth bs !internal_oranullval
    )
  );
  sth.binds <- (sth.binds +1);
  ()
      
(* parse a SQL statement - note that this does *not* validate the SQL in any way,
   it simply sets it in the statement handle's context *)

let define_select_cols sth =
  begin
    oci_statement_execute sth.parent_lda.lda sth.sth sth.parent_lda.auto_commit true; 
    let sql_cols = (oci_get_column_types sth.parent_lda.lda sth.sth)  in
    sth.num_cols <- Array.length sql_cols;
    Array.iteri (fun i x  ->
      match x with
	|Col_type (name, dtype, size, is_int, is_null) ->
	  Hashtbl.replace sth.defined_vals (Pos i) (oci_define sth.parent_lda.lda
                                               sth.sth i (dtype, is_int, is_null) size)
	| _ -> () 
    )  sql_cols
  end

let oraparse sth sqltext =
  let t1 = gettimeofday () in
  let sql_type = oci_statement_prepare sth.parent_lda.lda sth.sth sqltext in
  let t2 = gettimeofday () -. t1 in
  debug (sprintf "parsed sql \"%s\" of type %d on statement handle %d in %fs" sqltext sql_type sth.statement_id t2);

  (* if this is a select statement we will need to setup the defines so we can 
     fetch into it. execute with OCI_DESCRIBE_ONLY and get an array back. For
     each item in the array, allocate an appropriate define (C) and store a 
     pointer to it indexed by Pos n *)
  (match sql_type with
    |1 -> define_select_cols sth
    |_ -> ()
  );

  sth.sql_type <- sql_type;
  sth.parses <- (sth.parses + 1);
  sth.sth_op_time <- t2;
  sth.rows_affected <- 0;
  sth.out_pending <- false;
  sth.out_counter <- 0;
  Hashtbl.clear sth.bound_vals; Hashtbl.clear sth.oci_ptrs;
  ()
    
(* Execute the statement currently set in the statement handles. At this point,
   an exception may be throw if the SQL is invalid. Calling this before the
   statement is parsed will also result in an exception being thrown *)
let oraexec sth =
  let t1 = gettimeofday () in
  oci_set_prefetch sth.parent_lda.lda sth.sth sth.prefetch_rows;
  oci_sess_set_attr sth.parent_lda.lda oci_attr_action (sprintf "oraexec: starting %d" sth.statement_id);
  oci_statement_execute sth.parent_lda.lda sth.sth sth.parent_lda.auto_commit false;
  oci_sess_set_attr sth.parent_lda.lda oci_attr_action (sprintf "oraexec: completed %d" sth.statement_id);
  let t2 = gettimeofday () -. t1 in
  debug (sprintf "statement handle %d executed in %fs" sth.statement_id t2);
  sth.execs <- (sth.execs + 1);
  if sth.sql_type <> 1 then
    sth.rows_affected <- oci_get_rows_affected sth.parent_lda.lda sth.sth
  else
    ();
  sth.sth_op_time <- t2;
  ()

(* quick convenient function for binding an array of col_values to an sth and executing *)
let orabindexec_slow sth cval = 
  List.iter (fun cva -> Array.iteri (fun i v -> orabind sth (Pos (i + 1)) v) cva) cval;
  oraexec sth;
  ()

(* quick convenient function for just running one SQL statement *)
let orasql sth sqltext =
  oraparse sth sqltext;
  oraexec sth;
  ()

let oraclose sth = 
  debug (sprintf "freeing statement id %d from connection id %d" sth.statement_id sth.parent_lda.connection_id);
  Hashtbl.remove open_statements (sth.parent_lda.connection_id, sth.statement_id);
  Hashtbl.clear sth.bound_vals;
  oci_free_statement sth.sth

let make_new_statement statement_id parent_lda stmt =
  {statement_id=statement_id; 
   parses=0; binds=0; execs=0; sth_op_time=0.0; prefetch_rows = !oraprefetch_default; rows_affected=0; num_cols=0;
   out_pending=false; out_counter = 0; sql_type=0; out_types=(Hashtbl.create 10);
   bound_vals=(Hashtbl.create 10); defined_vals=(Hashtbl.create 10); oci_ptrs=(Hashtbl.create 10); 
   ref_cursors=(Hashtbl.create 10); parent_lda=parent_lda; sth=stmt}
    
(* open a statement handle/cursor on a given connection - actually allocated 
   from the global env's memory pool as the OCI sample code seems to do it 
   that way. Needs a *lot* of metadata to support the OraTcl style API *)
let oraopen lda =
  let s = oci_alloc_statement global_env in
  statement_seq := (!statement_seq + 1);
  let c = !statement_seq in
  debug (sprintf "allocated statement id %d on connection id %d" c lda.connection_id);
  let new_statement = make_new_statement c lda s in
  Hashtbl.add open_statements (lda.connection_id, c) new_statement;
  new_statement

(* connect to Oracle, connstr in format "user/pass@db" or "user/pass" like OraTcl *)
let oralogon connstr = 
  let t1 = gettimeofday () in
  let h = oci_alloc_handles global_env in
  let parse_connect_string c = sscanf c "%s@/%s@@%s"(fun u p d -> (u, p, d)) in
  let (username, password, database) = parse_connect_string connstr in 
  oci_server_attach h database;
  oci_sess_set_attr h oci_attr_username username;
  oci_sess_set_attr h oci_attr_password password;
  oci_session_begin h;
  handle_seq := (!handle_seq + 1);
  let c = !handle_seq in 
  let t2 = (gettimeofday () -. t1) in
  debug (sprintf "established connection %d as %s@%s in %fs" c username database t2);
  oraprompt := (sprintf "connected to %s@%s > " username database);
  let conn = {connection_id=c; commits=0; rollbacks=0; auto_commit=false; deq_timeout=(-1); lda_op_time=t2; lda=h} in
  Hashtbl.add open_connections c conn;
  conn

(* Disconnect from Oracle and release the memory. Global env is still allocated *)
let oralogoff lda =
  let c = lda.connection_id in
  oci_session_end lda.lda;
  oci_server_detach lda.lda;
  oci_free_handles lda.lda;
  Hashtbl.remove open_connections c;
  oraprompt := "not connected > ";
  debug (sprintf "disconnected %d" lda.connection_id);
  ()

(* commit work outstanding on this connection *)
let oracommit lda = 
  oci_commit lda.lda;
  lda.commits <- (lda.commits + 1);
  debug (sprintf "connection id %d committed transaction %d" lda.connection_id lda.commits);
  ()

(* rollback uncommitted work on this connection *)
let oraroll lda = 
  oci_rollback lda.lda;
  lda.rollbacks <- (lda.rollbacks + 1);
  debug (sprintf "connection id %d rolled back transaction, %d rollbacks in this session" lda.connection_id lda.rollbacks);
  ()

(* convert a col_value or a col_type to a string for display *)
let rec orastring c = 
  match c with
    |Varchar x  -> x
    |Integer x  -> (sprintf "%d" x)
    |Number x   -> (sprintf "%f" x)
    |Datetime x -> (sprintf "%02d-%s-%d %02d:%02d:%02d" x.tm_mday Log_message.months.(x.tm_mon) (x.tm_year+1900) x.tm_hour x.tm_min x.tm_sec)
    |Col_type (a,b,c,d,e) -> a
    |Null -> begin match !internal_oranullval with Null -> "null" | x ->
        orastring x end
    |RefCursor -> "#REF CURSOR#"
    |Statement _ -> "#STATEMENT#"
    |Binary _ -> "#BINARY DATA#"

(* describe a table - column names only (for now!) - using the implicit 
   describe method - also see implementation of oracols *)
let oradesc lda tabname =
  let sth = oraopen lda in
  ignore(oci_statement_prepare sth.parent_lda.lda sth.sth (sprintf "select * from %s" tabname)); (* discard return value here *)
  oci_statement_execute sth.parent_lda.lda sth.sth sth.parent_lda.auto_commit true; (* true - with OCI_DESCRIBE_ONLY set *)
  Array.map (fun x -> 
    match x with 
    |Col_type (col_name, col_type, col_size, is_int, is_null) 
      -> (col_name, 
	  (
	    match (col_type, is_int) with 
	    |(2, true) -> Integer 0
	    |(2, false) -> Number 0.0
	    |(1, _) -> Varchar ""
	    |(12,_) -> Datetime (localtime 0.0)
	    |_ -> Null
	  ), col_size - 1, (* because we add 1 in the OCI layer to cope with C's \0 *) 
	  (
	    match is_null with
	  |true -> Nullable
	  |false -> Not_nullable
	  )
      )
    |_ -> ("Unknown", Null, 0, Nullable)
  ) (oci_get_column_types lda.lda sth.sth) 

(* list of columns from last exec - this differs from oradesc in that it gives 
   all the columns in an actual query *)
let oracols sth = oci_get_column_types sth.parent_lda.lda sth.sth

(* get the date back from the C layer as a double, then convert it to Unix.tm *)
let oci_get_defined_date ptr =
  let d = oci_get_date_as_double ptr in
  localtime d

let ora_get_or_null null expr =
  if null then Null else
  try expr () with Oci_exception (22060, _) -> Null
                                      
(* call the underlying OCI fetch, advancing the cursor by one row, then extract 
   the data one column at a time from the define handles *)
let orafetch_select sth = 
  debug(sprintf "orafetch_select: entered rows_affected=%d" sth.rows_affected);
  try
    (match sth.rows_affected with
      |0 -> ()
      |_ -> oci_fetch sth.parent_lda.lda sth.sth);
    let row = Array.make sth.num_cols Null in
    (for i = 0 to (sth.num_cols - 1) do
       let {dtype=dt; is_int; is_null; ptr} = Hashtbl.find sth.defined_vals (Pos i) in
       match dt with
	  |1  -> row.(i) <- ora_get_or_null is_null @@ fun () -> Varchar (oci_get_defined_string ptr)
	  |12 -> row.(i) <- ora_get_or_null is_null @@ fun () -> Datetime (oci_get_defined_date ptr)
	  |2 -> (* could be an int or a float *)
	    (debug(sprintf "col=%d type=%d is_int=%b" i dt is_int);
	     match is_int with             
	      |true -> row.(i) <- ora_get_or_null is_null @@ fun () -> Integer (oci_get_int sth.parent_lda.lda ptr)
	      |false -> row.(i) <- ora_get_or_null is_null @@ fun () -> Number (oci_get_double sth.parent_lda.lda ptr)
	    )
	  |_ -> debug(sprintf "orafetch unhandled type in row %d col=%d datatype=%d is_int=%b" sth.rows_affected i dt is_int);
     done); 
    sth.rows_affected <- (sth.rows_affected + 1);
    debug(sprintf "orafetch: returning row %d" sth.rows_affected);
    row
  with Oci_exception (e_code, e_desc) ->
    ( match e_code with
      |1403 -> 
	debug (sprintf "orafetch: not found: rows=%d" sth.rows_affected); 
	raise Not_found
      |_    -> raise (Oci_exception (e_code, e_desc)))

(* build a result set from the out variables. we know how many we have from the 
   number of keys in sth.oci_ptrs. We know how many rows we have from 
   sth.rows_affected. So we need to loop and pivot.
*)
let orafetch_out sth =
  debug("orafetch_out: entered");
   let num_ptrs = Hashtbl.length sth.out_types in
   let rs = Array.make num_ptrs Null in
   let more_rows = (sth.out_counter < sth.rows_affected) in
   let i = ref 0 in
   debug(sprintf "num_ptrs=%d out_counter=%d rows_affected=%d more_rows=%b" num_ptrs sth.out_counter sth.rows_affected more_rows);
   (match more_rows with
     |true ->
       List.iter (fun bs ->
	 (match (Hashtbl.find sth.out_types bs) with
	   |Integer _ -> 
	     rs.(!i) <- Integer (oci_get_int_from_context sth.parent_lda.lda (Hashtbl.find sth.oci_ptrs bs) sth.out_counter);
	   |Number _ ->
	     rs.(!i) <- Number (oci_get_float_from_context sth.parent_lda.lda (Hashtbl.find sth.oci_ptrs bs) sth.out_counter);
	   |Varchar _ ->
	     begin
	       debug("orafetch_out: getting varchar");
	       rs.(!i) <- Varchar (oci_get_string_from_context sth.parent_lda.lda (Hashtbl.find sth.oci_ptrs bs) sth.out_counter);
	     end
	   |Datetime _ ->
	     begin
	       let epoch = oci_get_date_from_context sth.parent_lda.lda (Hashtbl.find sth.oci_ptrs bs) sth.out_counter in
	       debug(sprintf "gotten epoch time back as %f" epoch);
	       rs.(!i) <- Datetime (localtime epoch);
	     end
	   |RefCursor ->
	     (* get the oci_statement from sth.ref_cursors and turn it into a meta_statement *)
	     let s = (make_new_statement 99 sth.parent_lda (Hashtbl.find sth.ref_cursors bs)) in
	     define_select_cols s;
	     oci_fetch s.parent_lda.lda s.sth;
	     rs.(!i) <- Statement s
	   |_ -> ());
	 sth.out_counter <- (sth.out_counter +1); i := (!i + 1);
       ) (hash_keys sth.out_types) 
     |false ->
       (begin
	 debug("orafetch_out: end of result set");
	 sth.out_pending <- false;
	 sth.out_counter <- 0;
	 Hashtbl.clear sth.oci_ptrs; (* this *ought* to result in them being GC'd with the C callback... *)
	 raise Not_found;
	end));
   rs
	 
(* overload orafetch so it can be used for regular selects and for out variables *)
let orafetch sth = 
  match sth.out_pending with
    |false -> orafetch_select sth
    |true  -> orafetch_out sth

(* fetch all rows in a cursor and return them as a list - relying on prefetch to make this fast *)
let rec orafetchall_ sth acc =
  try
    orafetchall_ sth (acc @ [(orafetch sth)])
  with
    |Not_found -> acc
	  
let orafetchall sth =
  oci_sess_set_attr sth.parent_lda.lda oci_attr_action "orafetchall: starting";
  let rs = orafetchall_ sth [] in
  oci_sess_set_attr sth.parent_lda.lda oci_attr_action "orafetchall: done";
  rs

(* 0.2 functionality - object type AQ *)

(* Get the TDO of the message type with global env, handles (already unpacked) and type name in 
   UPPERCASE - returns a pointer to it in the OCI object cache *)
let oci_get_tdo ge lda tn =
  oci_get_tdo_ ge lda (String.uppercase tn)

let oci_int_from_payload lda pa i =
  oci_int_from_number lda.lda pa i

let oci_flt_from_payload lda pa i =
  oci_flt_from_number lda.lda pa i

let oci_string_from_payload pa i = 
  let sp = oci_read_ptr_at_offset pa i true in
  oci_string_from_string global_env sp

let calculate_aq_message_size size_of_pointer size_of_number payload =
  let total = ref 0 in
  Array.iter (fun x ->
    match x with
      |Integer v -> total := (!total + size_of_number)
      |Number  v -> total := (!total + size_of_number)
      |Varchar v -> total := (!total + size_of_pointer)
      |_ -> debug("This datatype not yet supported in AQ messages")
  ) payload;
	!total

(* build and enqueue an AQ message *)
let oraenqueue_obj lda queue_name message_type payload = 
  let ps = oci_size_of_pointer () in                                           (* pointer size - for OCIString *)
  let ns = oci_size_of_number () in                                            (* number size - for OCINumber *)
  let ni = Array.length payload in                                             (* number of payload items *)
  let pa = oci_alloc_c_mem (calculate_aq_message_size ps ns payload) in        (* payload array *)
  let na = oci_alloc_c_mem ((ni + 1) * ps) in                                  (* null array - fixed size *)
  let mt = oci_get_tdo global_env lda.lda message_type in                      (* message TDO pointer *)
  let co = ref 0 in                                                            (* current offset *)
  oci_constant_assign na 0 0;                                                  (* put OCI_IND_NOTNULL from oro.h at position 0 
										  (TDO) in the null array *)
  Array.iteri (fun i x ->
    match x with
      |Varchar v -> 
	begin
	  let s = oci_string_assign global_env lda.lda v in
	  oci_write_ptr_at_offset pa !co s;                                    (* write OCIString at current offset in 
										  payload array *)
	  debug(sprintf "read back string '%s' at offset %d as '%s'" v !co (oci_string_from_payload pa !co));
	  co := (!co + ps);                                                    (* increment the offset by the size of a 
										  pointer *)
	  oci_constant_assign na ((i + 1) * ps) 0                              (* write OCI_IND_NOTNULL at position i+1 in 
										  the null array *)
	end
      |Integer n -> 
	begin
	  oci_write_int_at_offset lda.lda pa !co n;                        (* copy the entire OCINumber into the 
										  payload *)
	  debug(sprintf "read back int %d at offset %d as %d\n" n !co (oci_int_from_payload lda pa !co));
	  co := (!co + ns);
	  oci_constant_assign na ((i + 1) * ps) 0;
	end
      |Number n ->
	begin
	  oci_write_flt_at_offset lda.lda pa !co n;
	  debug(sprintf "read back float %f at offset %d as %f\n" n !co (oci_flt_from_payload lda pa !co));
	  co := (!co + ns);
	  oci_constant_assign na ((i + 1) * ps) 0;
	end
      | _ -> oci_constant_assign na ((i + 1) * ps) (-1) (* OCI_IND_NULL *)
  ) payload;
  oci_aq_enqueue lda.lda queue_name mt pa na;
  ()

let oradequeue_obj lda queue_name message_type dummy_payload =
  let ps = oci_size_of_pointer () in                                           (* pointer size - for OCIString *)
  let ns = oci_size_of_number () in                                            (* number size - for OCINumber *)
  let ni = Array.length dummy_payload in                                       (* number of payload items *)
  let mt = oci_get_tdo global_env lda.lda message_type in                      (* message TDO pointer - should cache this in the lda *)
  let co = ref 0 in                                                            (* current offset *)
  let rv = Array.make ni Null in                                               (* array returned from function *)
  let pa = oci_aq_dequeue global_env lda.lda queue_name mt lda.deq_timeout in  (* payload array *)
  Array.iteri (fun i x -> 
    match x with 
      |Varchar z ->
	begin
	  debug(sprintf "oradequeue_obj: found Varchar, current offset is %d" !co);
	  rv.(i) <- Varchar (oci_string_from_payload pa !co);
	  co := (!co + ps);
	  debug(sprintf "oradequeue_obj: dequeued string '%s'" (orastring rv.(i)))
	  end
      |Integer z ->
	begin
	  debug(sprintf "oradequeue_obj: found Integer, current offset is %d" !co);
	  rv.(i) <- Integer (oci_int_from_payload lda pa !co);
	  co := (!co + ns);
	end
      |Number z ->
	begin
	  rv.(i) <- Number (oci_flt_from_payload lda pa !co);
	  co := (!co + ns);
	end;
      |_ -> debug("dequeue for this type not supported yet");
    ) dummy_payload;
  rv
  
let oraenqueue_raw lda queue_name message_type payload =
  let mt = oci_get_tdo global_env lda.lda "RAW" in
  match payload.(0) with
    |Binary b -> oci_aq_enqueue_raw global_env lda.lda queue_name mt b
    |_ -> raise (Invalid_argument "Cannot enqueue this message as RAW")
  
let oraenqueue lda queue_name message_type payload =
  match message_type with
    |"RAW" -> oraenqueue_raw lda queue_name message_type payload
    |_     -> oraenqueue_obj lda queue_name message_type payload

let oradequeue_raw lda queue_name = 
  let mt = oci_get_tdo global_env lda.lda "RAW" in
  [|Binary (oci_aq_dequeue_raw global_env lda.lda queue_name mt lda.deq_timeout)|]

let oradequeue lda queue_name message_type payload =
  debug(sprintf "oradequeue: queue_name='%s' message_type='%s'" queue_name message_type);
  try
    match message_type with
      |"RAW" -> oradequeue_raw lda queue_name
      |_     -> oradequeue_obj lda queue_name message_type payload
  with
      Oci_exception (e_code, e_desc) ->
	match e_code with
	  |25228 -> raise Not_found (* nothing on the queue and timeout set *)
	  |_     -> raise (Oci_exception (e_code, e_desc))

(* 0.2.2 OUT binds - also see orafetch modifications above *)
let rec orabindout sth bs cv = 
  begin
    match bs with
      |Pos p ->
	let bh = 
	  (try
	     Hashtbl.find sth.bound_vals bs
	   with
	   Not_found -> (let bh = oci_alloc_bindhandle () in
			 Hashtbl.add sth.bound_vals bs bh;
			 bh)) in
	begin
	  match cv with
	    |Integer _ |Number _ ->
	      begin
		Hashtbl.replace sth.oci_ptrs bs (oci_bind_numeric_out_by_pos sth.parent_lda.lda sth.sth bh p);
		Hashtbl.replace sth.out_types bs cv;
	      end
	    |Datetime _ ->
	      begin
		Hashtbl.replace sth.oci_ptrs bs (oci_bind_date_out_by_pos sth.parent_lda.lda sth.sth bh p);
		Hashtbl.replace sth.out_types bs cv;
	      end
	    |Varchar _ ->
	      begin
		Hashtbl.replace sth.oci_ptrs bs (oci_bind_string_out_by_pos sth.parent_lda.lda sth.sth bh p);
		Hashtbl.replace sth.out_types bs cv;
	      end
	    |RefCursor ->
	      begin
		let r = oci_alloc_statement global_env in 
		Hashtbl.replace sth.ref_cursors bs r;
		Hashtbl.replace sth.out_types bs cv;
		(* bind r itself *)
		oci_bind_ref_cursor sth.parent_lda.lda sth.sth bh p r;
	      end
	    |_ -> debug("orabindout: this type not implemented yet")
	end
      |Name n ->
	let un = String.uppercase n in
	let ucn = (match (String.sub un 0 1) with 
	  |":" -> (String.sub un 1 ((String.length un) - 1))
	  |_   -> un) in
	orabindout sth (Pos (oci_get_pos_from_name sth.parent_lda.lda sth.sth ucn)) cv
  end;
  sth.binds <- (sth.binds + 1);
  sth.out_pending <- true;
  ()

(* bulk DML implementation of orabindexec *)
let orabindexec_bulk sth cval =
  let batch_size = List.length cval in
  let first_row = List.hd cval in
  let num_cols = Array.length first_row in
  debug (sprintf "orabindexec_bulk: batch_size=%d num_cols=%d" batch_size num_cols);
  oci_sess_set_attr sth.parent_lda.lda oci_attr_action "orabindexec_bulk: running";
  (* allocate bind handles for each position *)
  Hashtbl.clear sth.bound_vals; Hashtbl.clear sth.oci_ptrs;
  for i = 1 to num_cols do
    Hashtbl.add sth.bound_vals (Pos i) (oci_alloc_bindhandle ())
  done;
  
  (* get the sizes of each of the 4 types, and the longest string - since i am expecting
     the bulk of the work to be on the Oracle side, the speedup of just assuming 4k is not
     worth it in memory usage *)
  let int_size = oci_size_of_int () in
  let float_size = oci_size_of_float () in
  let date_size = oci_size_of_date () in
  let pointer_size = oci_size_of_pointer () in 
  let string_size = ref 0 in
  List.iter (fun item ->
    Array.iter (fun member ->
      match member with
	|Varchar v ->
	  let l = String.length v in
	  if (l > !string_size) 
	  then string_size := l
	  else ()
	|_ -> ()
    ) item
  ) cval;
  
  (* align the string size *)
  string_size := ((!string_size/pointer_size) + 1) * pointer_size;
  debug(sprintf "orabindexec_bulk: int_size=%d float_size=%d date_size(aligned)=%d string_size(aligned)=%d" int_size float_size date_size !string_size);

  (* for each column, allocate enough storage for the batch *)
  Array.iteri (fun i x -> 
    Hashtbl.add sth.oci_ptrs (Pos (i + 1)) 
      (oci_alloc_c_mem 
	 (match x with
	   |Integer _  -> batch_size * int_size
	   |Number _   -> batch_size * float_size
	   |Varchar _  -> batch_size * !string_size
	   |Datetime _ -> batch_size * date_size
	   |_ -> debug ("orabindexec_bulk: unknown type"); 0
	 )
      )
  ) first_row;
    
  (* for each column in each row, get its oci_ptr from its bind spec, then 
     write it at size*row_num bytes offset *)
  let row_count = ref 0 in
  List.iter (fun row -> 
      Array.iteri (fun i x -> 
	let ptr = (Hashtbl.find sth.oci_ptrs (Pos (i + 1))) in
	match x with
	  |Integer i  -> oci_write_nat_int_at_offset ptr (!row_count * int_size) i
	  |Number n   -> oci_write_nat_flt_at_offset ptr (!row_count * float_size) n
	  |Varchar v  -> oci_write_chr_at_offset ptr (!row_count * !string_size) v
	  |Datetime d -> oci_write_odt_at_offset ptr (!row_count * date_size) (date_to_double d)
	  |_ -> debug ("orabindexec_bulk: unknown type")
	
      ) row;
    row_count := (!row_count +1);
  ) cval;

  (* bind each pointer to the corresponding bindhandle *)
  Array.iteri (fun i x -> 
    let bs = (Pos (i + 1)) in
    let bh = Hashtbl.find sth.bound_vals bs in
    let ptr = Hashtbl.find sth.oci_ptrs bs in
    (match x with
      |Integer _  -> oci_bind_bulk_int sth.parent_lda.lda sth.sth bh ptr (i + 1)
      |Number _   -> oci_bind_bulk_flt sth.parent_lda.lda sth.sth bh ptr (i + 1)
      |Varchar _  -> oci_bind_bulk_chr sth.parent_lda.lda sth.sth bh ptr (!string_size, (i + 1))
      |Datetime _ -> oci_bind_bulk_odt sth.parent_lda.lda sth.sth bh ptr (date_size,  (i + 1))
      |_ -> ());
    debug(sprintf "orabindexec_bulk: done bind at position %d type %s" (i + 1) (match x with
    |Integer _ -> "Integer"
    |Number _ -> "Number"
    |Varchar _ -> "Varchar"
    |Datetime _ -> "Datetime"
    |_ -> "Unknown"
    )
    )
  ) first_row;

  (* finally bulk execute *)
  oci_bulk_exec sth.parent_lda.lda sth.sth batch_size sth.parent_lda.auto_commit;
  sth.rows_affected <- oci_get_rows_affected sth.parent_lda.lda sth.sth;
  oci_sess_set_attr sth.parent_lda.lda oci_attr_action "orabindexec_bulk: done";
  ()
let orabindexec = orabindexec_bulk

(* End of file *)


