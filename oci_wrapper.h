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
#define Oci_statement_val(v) (*((OCIStmt**) Data_custom_val(v)))
#define Oci_bindhandle_val(v) (*((OCIBind**) Data_custom_val(v)))
#define Oci_date_val(v) (*((OCIDate**) Data_custom_val(v)))

/* we will never do comparison on any of the OCI env/conn types. This results
   in a compilation error as it's not used in this file, but it is used elsewhere */
static struct custom_operations oci_custom_ops = {"oci_custom_ops", NULL, NULL, NULL, NULL, NULL};

/* declare common C functions (not called directly from OCaml) */
void debug(char* msg);
void raise_caml_exception(int exception_code, char* exception_string);
void oci_non_success(oci_handles_t h);
void run_sql_simple(OCIEnv* e, oci_handles_t h, char* sql);

/* type conversion functions */
void epoch_to_ocidate(double d, OCIDate* ocidate);

/* end of file */
