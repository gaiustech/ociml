/* include file for OCI wrapper */

#define CHECK_OCI(x, h) if (x != OCI_SUCCESS) { oci_non_success(h); }

/* works on linux (!) */
#ifdef DEBUG
#define BREAKPOINT asm("int3"); 
#else
#define BREAKPOINT /**/
#endif

/* Allocate the handles for error, server, service context and session in a struct for convenience */
typedef struct {
  OCIError*   err;
  OCIServer*  srv;
  OCISvcCtx*  svc;
  OCISession* ses;
} oci_handles_t;

/* struct for defining for rows fetched */
typedef struct {
  OCIDefine* defh;
  void* ptr; /* the data itself */
  int dtype;
  double dbl;
} oci_define_t;

typedef struct {
  void* ptr;
} c_alloc_t;

/* struct for storing the callback data */
typedef struct {
  int indicator;
  int rc;      /* return code */
  OCINumber bufpp; /* pointer to the data */
  int alenp;   /* actual length */
} out_data_t;

/* struct for context for dynamic bind callback */
typedef struct {
  c_alloc_t cht;
  OCIError* err;
} cb_context_t;

#define Oci_env_val(v)        (*((OCIEnv**)       Data_custom_val(v)))
#define Oci_handles_val(v)    (*((oci_handles_t*) Data_custom_val(v)))
#define Oci_statement_val(v)  (*((OCIStmt**)      Data_custom_val(v)))
#define Oci_bindhandle_val(v) (*((OCIBind**)      Data_custom_val(v)))
#define Oci_date_val(v)       (*((OCIDate**)      Data_custom_val(v)))
#define Oci_defhandle_val(v)  (*((oci_define_t*)  Data_custom_val(v)))
#define C_alloc_val(v)        (*((c_alloc_t*)     Data_custom_val(v)))
#define C_context_val(v)      (*((cb_context_t*)  Data_custom_val(v)))

/* declare common C functions (not called directly from OCaml) */
void debug(char* msg);
void raise_caml_exception(int exception_code, char* exception_string);
void oci_non_success(oci_handles_t h);
void run_sql_simple(OCIEnv* e, oci_handles_t h, char* sql);

/* type conversion functions */
void epoch_to_ocidate(double d, OCIDate* ocidate);
double ocidate_to_epoch(OCIDate* ocidate);

/* memory */
void caml_free_alloc_t(value ch);

/* end of file */
