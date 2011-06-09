(* various utility functions and type definitions *)

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
let oci_attr_action             = 367

(* various constants from ocidfn.h *)
let oci_sqlt_odt                = 156 (* OCIDate object *)
let oci_sqlt_str                = 5   (* zero-terminated string *)
let oci_sqlt_int                = 3   (* integer *)
let oci_sqlt_flt                = 4   (* floating point number *)
let oci_sqlt_num                = 2   (* ORANET numeric *)
let oci_sqlt_dat                = 12  (* Oracle 7-byte date *)
let oci_sqlt_chr                = 1   (* ORANET character string *)

(* end of file *)
