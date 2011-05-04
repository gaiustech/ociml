/* C stubs for OCaml OCI wrapper */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <string.h>
#include <stdio.h>
#include <oci.h>

/* kick an error back into OCaml-land */
void raise_caml_exception(int exception_code, char* exception_string) {
  CAMLlocal1(e);
  e = caml_alloc_tuple(2);
  Store_field(e, 0, Val_long(exception_code));
  Store_field(e, 1, caml_copy_string(exception_string));
  caml_raise_with_arg(*caml_named_value("Ociml_exception"), e);
}  

/* Initialize the OCI library */
value caml_oci_initialize(value unit) {
  CAMLparam1(unit);
  OCIInitialize((ub4) OCI_DEFAULT, (dvoid *)0,
                       (dvoid * (*)(dvoid *, size_t)) 0,
                       (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                       (void (*)(dvoid *, dvoid *)) 0 );
  CAMLreturn(Val_unit);
}

/* Create an OCI environment */
#define Oci_env_val(v) (*((OCIEnv**) Data_custom_val(v)))
/* we will never do comparison on any of the OCI env/conn types */
static struct custom_operations oci_custom_ops = {"oci_custom_ops", NULL, NULL, NULL, NULL, NULL};

value caml_oci_env_create(value unit) {
  CAMLparam1(unit);
  OCIEnv* env;
  sword x = OCIEnvCreate(&env, OCI_DEFAULT, 0, 0, 0, 0, 0, 0);
  if (x != OCI_SUCCESS) {
    raise_caml_exception(-1, "Cannot create an OCI environment (check ORACLE_HOME?)");
  }
  
  value v = caml_alloc_custom(&oci_custom_ops, sizeof(OCIEnv*), 0, 1);
  Oci_env_val(v) = env;
  CAMLreturn(v);
}

/* Allocate the handles for error, server, service context and session in a struct for convenience */
typedef struct {
  OCIError*   err;
  OCIServer*  srv;
  OCISvcCtx*  svc;
  OCISession* ses;
} oci_handles_t;

#define Oci_handles_val(v) (*((oci_handles_t*) Data_custom_val(v)))
value caml_oci_alloc_handles(value env) {
  CAMLparam1(env);
  OCIEnv* e = Oci_env_val(env);

  oci_handles_t h = { NULL, NULL, NULL, NULL };

  OCIHandleAlloc(e, (dvoid**)&h.err, OCI_HTYPE_ERROR,   0, 0);
  OCIHandleAlloc(e, (dvoid**)&h.srv, OCI_HTYPE_SERVER,  0, 0);
  OCIHandleAlloc(e, (dvoid**)&h.svc, OCI_HTYPE_SVCCTX,  0, 0);
  OCIHandleAlloc(e, (dvoid**)&h.ses, OCI_HTYPE_SESSION, 0, 0);

  value v = caml_alloc_custom(&oci_custom_ops, sizeof(oci_handles_t), 0, 1);
  Oci_handles_val(v) = h;
  CAMLreturn(v);
}

/* Attach to Oracle but do not authenticate (yet) */
value caml_oci_server_attach(value handles, value dbname) {
  CAMLparam2(handles, dbname);
  oci_handles_t h = Oci_handles_val(handles);
  char* db = String_val(dbname);

  sword x = OCIServerAttach(h.srv, h.err, (text*)db, strlen(db), OCI_DEFAULT);
  if (x != OCI_SUCCESS) {
    char* buf = (char*)malloc(256);
    snprintf(buf, 255, "Cannot attach to server \"%s\"", db);
    raise_caml_exception(-1, buf);
  }

  CAMLreturn(Val_unit);
}

/* end of file */
