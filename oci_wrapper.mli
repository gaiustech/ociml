(* Fairly thin wrapper around low-level OCI functions, used to implement higher-level OCI/ML library *)

(* OCI handles that have to be tracked at the application level *)
type oci_env (* global OCI environment *)
type oci_handles (* C struct that bundles error, server, service context and session handles *)

(* setup functions, in order in which they should be called *)
external oci_initialize: unit -> unit = "caml_oci_initialize"
external oci_env_create: unit -> oci_env = "caml_oci_env_create"
external oci_alloc_handles: oci_env -> oci_handles = "caml_oci_alloc_handles"
external oci_server_attach: oci_handles -> string -> unit = "caml_oci_server_attach" (* takes db name *)
external oci_sess_set_attr: oci_handles -> int -> string -> unit = "caml_oci_sess_set_attr" 
external oci_session_begin: oci_handles -> unit = "caml_oci_session_begin" (* username and password set as attrs *)
external oci_set_module: oci_env -> oci_handles -> string -> unit = "caml_oci_set_module"

external oci_session_end: oci_handles -> unit = "caml_oci_session_end"
external oci_server_detach: oci_handles -> unit = "caml_oci_server_detach"
external oci_terminate: unit -> unit = "caml_oci_terminate" (* final cleanup *)

(* query functions *)

(* End of file *)
