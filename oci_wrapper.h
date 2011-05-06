/* include file for OCI wrapper */

/* Allocate the handles for error, server, service context and session in a struct for convenience */
typedef struct {
  OCIError*   err;
  OCIServer*  srv;
  OCISvcCtx*  svc;
  OCISession* ses;
} oci_handles_t;

#define Oci_env_val(v) (*((OCIEnv**) Data_custom_val(v)))
#define Oci_handles_val(v) (*((oci_handles_t*) Data_custom_val(v)))

/* declare common C functions (not called directly from OCaml) */
void debug(char* msg);
void raise_caml_exception(int exception_code, char* exception_string);
void oci_non_success(oci_handles_t h);
void run_sql_simple(OCIEnv* e, oci_handles_t h, char* sql);

/* end of file */
