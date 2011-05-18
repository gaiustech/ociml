(* various utility functions and type definitions *)

open Unix

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

type meta_aq_msg = {msg_type:string; (* type name in the database *)
		    msg_tdo:oci_ptr; (* pointer to TDO *)
		    msg_items:col_value array; (* the actual message *)
		    item_ptrs:oci_ptr list;}

(* C heap memory functions - oci_common.c *)
external oci_alloc_c_mem: int -> oci_ptr = "caml_alloc_c_mem"
external oci_size_of_pointer: unit -> int = "caml_oci_size_of_pointer"

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
	  	  
let oci_c_alloc bytes = 
  let ptr = oci_alloc_c_mem bytes in
  {c_ptr=ptr;bytes_alloc=bytes;bytes_offset=0} 

(* end of file *)
