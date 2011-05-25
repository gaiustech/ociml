(* Oracle API for OCaml vaguely based on OraTcl *)

open Unix
open Printf
open Scanf
open Log_message
open Ociml_utils

(* Fairly thin wrapper around low-level OCI functions, used to implement higher-level OCI*ML library *)

(* OCI handles that have to be tracked at the application level *)
type oci_env (* global OCI environment *)
type oci_handles (* C struct that bundles error, server, service context and session handles *)
type oci_statement (* statement handle *)
type oci_bindhandle (* for binding in prepared statements *)
type oci_ptr (* void* pointer so we can heap alloc for binding/defining *)

(* data structure for use within the library bundling all the handles associated 
   with a connection with a unique identifier and some useful statistics *)
type meta_handle = {connection_id:int; 
		    mutable commits:int; 
		    mutable rollbacks:int;
		    mutable lda_op_time:float;
		    mutable auto_commit:bool;
		    lda:oci_handles}

(* Variant for the basic data types - datetime crosses back and forth as epoch, 
   and Oracle's NUMBER datatype of course can be either integer or floating 
   point but it doesn't make sense to make the OCaml layer deal only in floats *)
type col_value = Varchar of string|Datetime of Unix.tm|Integer of int|Number of float|Null

(* type for column metadata - name, type, size, is_integer, is_nullable *)
type col_type = string * int * int * bool * bool

(* for conversion to string *)
type stringable = Col_value of col_value|Col_type of col_type

(* variant enabling binding by position or by name *)
type bind_spec = Pos of int|Name of string

(* define includes datatype for later fetching *)
type define_spec = int * bool * oci_ptr

(* same with statements, counters for parses, binds and execs, and the parent 
   connection (as it is allocated from the global OCI environment) *)
type meta_statement = {statement_id:int; 
		       mutable parses:int;
		       mutable binds:int;
		       mutable execs:int;
		       mutable sth_op_time:float;
		       mutable rows_affected:int;
		       mutable num_cols:int;
		       bound_vals:(bind_spec, oci_bindhandle) Hashtbl.t;
		       defined_vals:(bind_spec, define_spec) Hashtbl.t;
		       oci_ptrs:(bind_spec, oci_ptr) Hashtbl.t;
		       parent_lda:meta_handle; 
		       sth:oci_statement}

(* pointer to C memory, with an offset to memory used so far *)
type meta_c_alloc = {c_ptr:oci_ptr;
		     bytes_alloc:int;
		     mutable bytes_offset:int}

(* various constants from oci.h *)
let oci_attr_username           =  22
let oci_attr_password           =  23 
let oci_attr_client_identifier  = 278 
let oci_attr_client_info        = 368 
let oci_attr_module             = 366
let oci_attr_rows_fetched       = 197
let oci_attr_prefetch_memory    = 13
let oci_attr_param_count        = 18
let oci_stmt_select             = 1

(* various constants from ocidfn.h *)
let oci_sqlt_odt                = 156 (* OCIDate object *)
let oci_sqlt_str                = 5   (* zero-terminated string *)
let oci_sqlt_int                = 3   (* integer *)
let oci_sqlt_flt                = 4   (* floating point number *)
let oci_sqlt_num                = 2   (* ORANET numeric *)
let oci_sqlt_dat                = 12  (* Oracle 7-byte date *)
let oci_sqlt_chr                = 1   (* ORANET character string *)

let date_to_double t =
  fst (mktime t)

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
external oci_get_column_types: oci_handles -> oci_statement -> col_type array = "caml_oci_get_column_types"
external oci_define: oci_handles -> oci_statement -> int -> (int * bool) -> int -> define_spec = "caml_oci_define"
external oci_fetch: oci_handles -> oci_statement -> unit = "caml_oci_fetch"

(* type conversions - oci_types.c *)
external oci_get_defined_string: oci_ptr -> string = "caml_oci_get_defined_string"
external oci_get_date_as_double: oci_ptr -> float = "caml_oci_get_date_as_double"
external oci_get_double: oci_handles -> oci_ptr -> float = "caml_oci_get_double"
external oci_get_int: oci_handles -> oci_ptr -> int = "caml_oci_get_int"

(* C heap memory functions - oci_common.c *)
external oci_alloc_c_mem: int -> oci_ptr = "caml_alloc_c_mem"
external oci_size_of_pointer: unit -> int = "caml_oci_size_of_pointer"
external oci_size_of_number: unit -> int = "caml_oci_size_of_number"
external oci_constant_assign: oci_ptr -> int -> int -> unit = "c_write_int_at_offset"
external oci_write_ptr_at_offset: oci_ptr -> int -> oci_ptr -> unit = "caml_write_ptr_at_offset"
external oci_read_ptr_at_offset: oci_ptr -> int -> oci_ptr = "caml_read_ptr_at_offset"
external oci_write_int_at_offset: oci_handles -> oci_ptr -> int -> int -> unit = "caml_oci_write_int_at_offset"
external oci_write_flt_at_offset: oci_handles -> oci_ptr -> int -> float -> unit = "caml_oci_write_flt_at_offset"

(* AQ functions - oci_aq.c *)
external oci_get_tdo_: oci_env -> oci_handles -> string -> oci_ptr = "caml_oci_get_tdo"
external oci_string_assign: oci_env -> oci_handles -> string -> oci_ptr = "caml_oci_string_assign_text"
external oci_aq_enqueue: oci_handles -> string -> oci_ptr -> oci_ptr -> oci_ptr -> unit = "caml_oci_aq_enqueue"
external oci_int_from_number: oci_handles -> oci_ptr -> int -> int = "caml_oci_int_from_number"
external oci_flt_from_number: oci_handles -> oci_ptr -> int -> float = "caml_oci_flt_from_number"
external oci_string_from_string: oci_env -> oci_ptr -> string = "caml_oci_string_from_string"
external oci_aq_dequeue: oci_handles -> string -> oci_ptr -> oci_ptr -> oci_ptr -> unit = "caml_oci_aq_dequeue"

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
  val orabindexec:  meta_statement -> col_value array -> unit
  val orastring:    stringable -> string
  val oradesc:      meta_handle -> string -> string array
  val oracols:      meta_statement -> string array
  val orafetch:     meta_statement -> col_value array
  val oranullval:   col_value -> unit
  val oraenqueue:   meta_handle -> string -> string -> col_value array -> unit
  val oradequeue:   meta_handle -> string -> string -> col_value array -> col_value array
end

(* actual implementation *)

let handle_seq = ref 0 (* unique ids for handles *)
let statement_seq = ref 0 (* unique ids for statements *)

(* write a timestamped log message (log messages from the C code are tagged {C} 
   so anything else is from the ML. This can be set from the application.  *)
let internal_oradebug = ref false
let oradebug x = internal_oradebug := x; ()
let debug msg = match !internal_oradebug with |true -> log_message msg |false -> ()

(* autocommit mode - default false *)
let oraautocom lda x = 
  lda.auto_commit <- x; ()

(* set this to what you want NULLs to be returned as, e.g. Integer 0 or Varchar "" or Datetime 0.0 even! *)
let internal_oranullval = ref (Integer 0)
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
  (match bs with 
    |Pos p -> debug(sprintf "orabind: p=%d" p) 
    |Name n -> ()
  );
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
	|Null       -> orabind sth bs !internal_oranullval
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
	|Null       -> orabind sth bs !internal_oranullval
    )
  );
  sth.binds <- (sth.binds +1);
  ()
      
(* parse a SQL statement - note that this does *not* validate the SQL in any way,
   it simply sets it in the statement handle's context *)
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
    |1 -> (
      oci_statement_execute sth.parent_lda.lda sth.sth sth.parent_lda.auto_commit true; 
      let sql_cols = oci_get_column_types sth.parent_lda.lda sth.sth in
      sth.num_cols <- Array.length sql_cols;
      Array.iteri (fun i (name, dtype, size, is_int, is_null) -> 
	Hashtbl.replace sth.defined_vals (Pos i) (oci_define sth.parent_lda.lda sth.sth i (dtype, is_int) size)) sql_cols)
    |_ -> ()
  );

  sth.parses <- (sth.parses + 1);
  sth.sth_op_time <- t2;
  sth.rows_affected <- 0;
  Hashtbl.clear sth.bound_vals;
  ()
    
(* Execute the statement currently set in the statement handles. At this point,
   an exception may be throw if the SQL is invalid. Calling this before the
   statement is parsed will also result in an exception being thrown *)
let oraexec sth =
  let t1 = gettimeofday () in
  oci_statement_execute sth.parent_lda.lda sth.sth sth.parent_lda.auto_commit false;
  let t2 = gettimeofday () -. t1 in
  debug (sprintf "statement handle %d executed in %fs" sth.statement_id t2);
  sth.execs <- (sth.execs + 1);
  sth.sth_op_time <- t2;
  ()

(* quick convenient function for binding an array of col_values to an sth and executing *)
let orabindexec sth cva = 
  Array.iteri (fun i v -> orabind sth (Pos (i + 1)) v) cva;
  oraexec sth;
  ()

(* quick convenient function for just running one SQL statement *)
let orasql sth sqltext =
  oraparse sth sqltext;
  oraexec sth;
  ()

let oraclose sth = 
  debug (sprintf "freeing statement id %d from connection id %d" sth.statement_id sth.parent_lda.connection_id);
  Hashtbl.clear sth.bound_vals;
  oci_free_statement sth.sth

(* open a statement handle/cursor on a given connection - actually allocated 
   from the global env's memory pool as the OCI sample code seems to do it 
   that way *)
let oraopen lda =
  let s = oci_alloc_statement global_env in
  statement_seq := (!statement_seq + 1);
  let c = !statement_seq in
  debug (sprintf "allocated statement id %d on connection id %d" c lda.connection_id);
  {statement_id=c; 
   parses=0; binds=0; execs=0; sth_op_time=0.0; rows_affected=0; num_cols=0; 
   bound_vals=(Hashtbl.create 10); 
   defined_vals=(Hashtbl.create 10); 
   oci_ptrs=(Hashtbl.create 10); 
   parent_lda=lda; sth=s}

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
  {connection_id=c; commits=0; rollbacks=0; auto_commit=false; lda_op_time=t2; lda=h}

(* Disconnect from Oracle and release the memory. Global env is still allocated *)
let oralogoff lda =
  oci_session_end lda.lda;
  oci_server_detach lda.lda;
  oci_free_handles lda.lda;
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
    |Col_type t  -> ((fun (x, _, _, _, _) -> x) t)
    |Col_value x -> 
      match x with
	|Varchar x  -> x
	|Integer x  -> (sprintf "%d" x)
	|Number x   -> (sprintf "%f" x)
	|Datetime x -> let months = [|"JAN"; "FEB"; "MAR"; "APR"; "MAY"; "JUN"; "JUL"; "AUG"; "SEP"; "OCT"; "NOV"; "DEC"|] in
		       (sprintf "%02d-%s-%d %02d:%02d:%02d" x.tm_mday months.(x.tm_mon) (x.tm_year+1900) x.tm_hour x.tm_min x.tm_sec)
	|Null -> orastring (Col_value !internal_oranullval)
	  
(* describe a table - column names only (for now!) - using the implicit describe method - also see implementation of oracols *)
let oradesc lda tabname =
  let sth = oraopen lda in
  ignore(oci_statement_prepare sth.parent_lda.lda sth.sth (sprintf "select * from %s" tabname)); (* discard return value here *)
  oci_statement_execute sth.parent_lda.lda sth.sth sth.parent_lda.auto_commit true; (* true - with OCI_DESCRIBE_ONLY set *)
  oci_get_column_types lda.lda sth.sth 

(* list of columns from last exec - this differs from oradesc in that it gives all the columns in an actual query *)
let oracols sth = Array.map (fun x -> Col_type x) (oci_get_column_types sth.parent_lda.lda sth.sth)

(* get the date back from the C layer as a double, then convert it to Unix.tm *)
let oci_get_defined_date ptr =
  let d = oci_get_date_as_double ptr in
  localtime d

(* call the underlying OCI fetch, advancing the cursor by one row, then extract the data one column at a time from the define handles *)
let orafetch sth = 
  debug("orafetch: entered");
  try
    (match sth.rows_affected with
      |0 -> ()
      |_ -> oci_fetch sth.parent_lda.lda sth.sth);
    let row = Array.make sth.num_cols (Col_value Null) in
    (for i = 0 to (sth.num_cols - 1) do
	let (dt, is_int, ptr) = Hashtbl.find sth.defined_vals (Pos i) in
	match dt with
	  |1  -> row.(i) <- Col_value (Varchar  (oci_get_defined_string ptr))
	  |12 -> row.(i) <- Col_value (Datetime (oci_get_defined_date ptr))
	  |2 -> (* could be an int or a float *) 
	    (debug(sprintf "col=%d type=%d is_int=%b" i dt is_int);
	     match is_int with
	      |true  -> row.(i) <- Col_value (Integer (oci_get_int    sth.parent_lda.lda ptr))
	      |false -> row.(i) <- Col_value (Number  (oci_get_double sth.parent_lda.lda ptr))
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

(* fetch all rows in a cursor and return them as a list *)
let rec orafetchall_ sth acc =
  try
    orafetchall_ sth (acc @ [(orafetch sth)])
  with
    |Not_found -> acc
	  
let orafetchall sth = 
  orafetchall_ sth []

(* Get the TDO of the message type with global env, handles (already unpacked) and type name in 
   UPPERCASE - returns a pointer to it in the OCI object cache *)
let oci_get_tdo ge lda tn =
  oci_get_tdo_ ge lda (String.uppercase tn)

let oci_int_from_payload lda pa i =
  oci_int_from_number lda.lda pa i

let oci_flt_from_payload lda pa i =
  oci_flt_from_number lda.lda pa i

let oci_string_from_payload pa i = 
  let sp = oci_read_ptr_at_offset pa i in
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
  let pa = oci_alloc_c_mem (calculate_aq_message_size ps ns dummy_payload) in  (* payload array *)
  let na = oci_alloc_c_mem ((ni + 1) * ps) in                                  (* null array - fixed size - need to handle this? *)
  let mt = oci_get_tdo global_env lda.lda message_type in                      (* message TDO pointer *)
  let co = ref 0 in                                                            (* current offset *)
  let rv = Array.make ni Null in                                               (* array returned from function *)
  oci_aq_dequeue lda.lda queue_name mt pa na;
  Array.iteri (fun i x -> 
    match x with 
      |Varchar z ->
	begin
	  rv.(i) <- Varchar (oci_string_from_payload pa !co);
	  co := (!co + ps);
	  debug(sprintf "oradequeue_obj: dequeued string '%s'" (orastring (Col_value rv.(i))))
	  end
      |Integer z ->
	begin
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
    
let oraenqueue lda queue_name message_type payload =
  match message_type with
    |"RAW" -> debug("Raw AQ not implemented yet")
    |_     -> oraenqueue_obj lda queue_name message_type payload

let oradequeue lda queue_name message_type payload =
  match message_type with
    |"RAW" -> debug("Raw AQ not implemented yet"); [|Null|];
    |_     -> oradequeue_obj lda queue_name message_type payload

(* End of file *)
