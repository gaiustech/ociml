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
  OCIAttrSet((void*)h.svc, (ub4) OCI_HTYPE_SVCCTX, (void*)h.srv, (ub4) 0, (ub4) OCI_ATTR_SERVER, h.err);
  CAMLreturn(Val_unit);
}

/* set an attribute within a session */
value caml_oci_sess_set_attr(value handles, value attr_name, value attr_value) {
  CAMLparam3(handles, attr_name, attr_value);
  oci_handles_t h = Oci_handles_val(handles);
  char* v = String_val(attr_value);
  int n = Int_val(attr_name);
  OCIAttrSet(h.ses, OCI_HTYPE_SESSION,(void *)v, (ub4)strlen(v), n, h.err);
  
  CAMLreturn(Val_unit);
}

/* Actually start a session. It is probably quite inefficient to keep coercing 
   my handles across the C/OCaml boundary but other than making them global
   and forfeiting having multiple sessions open I'm not sure what to do */
value caml_oci_session_begin(value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);
  OCIAttrSet ((void*)h.svc, OCI_HTYPE_SVCCTX, (void*)h.srv, (ub4) 0, OCI_ATTR_SERVER,h.err);
  sword x = OCISessionBegin ((void*)h.svc, h.err, h.ses, OCI_CRED_RDBMS, OCI_DEFAULT);
  if (x != OCI_SUCCESS)  {
    text* errbuf = (text*)malloc(512);
    sb4 errcode;
    OCIErrorGet ((void*) h.err, (ub4) 1, (text*) NULL, &errcode, errbuf, (ub4) sizeof(errbuf), (ub4) OCI_HTYPE_ERROR);
    raise_caml_exception((int)errcode, (char*)errbuf);
  }
  OCIAttrSet ((void*)h.svc, OCI_HTYPE_SVCCTX, (void*)h.ses, (ub4) 0, OCI_ATTR_SESSION, h.err);
  CAMLreturn(Val_unit);
}

  /* now do a pointless query to force v$session.module to update */
value caml_oci_set_module(value env, value handles, value module) {
  CAMLparam3(env, handles, module);
  OCIEnv* e = Oci_env_val(env);
  oci_handles_t h = Oci_handles_val(handles);
  char* m = String_val(module);

  OCIAttrSet(h.ses, OCI_HTYPE_SESSION,(void *)m, (ub4)strlen(m),OCI_ATTR_MODULE , h.err);

  char* sql = "commit;";
  OCIStmt* sth = NULL;
  OCIHandleAlloc((dvoid*)e, (dvoid**) &sth, OCI_HTYPE_STMT, 0, (dvoid**) 0);  
  OCIStmtPrepare(sth, h.err, (text*)sql, strlen(sql), OCI_NTV_SYNTAX, OCI_DEFAULT);
  OCIStmtExecute(h.svc,sth, h.err, (ub4) 1, (ub4) 0, (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT);

  CAMLreturn(Val_unit);
}

/* end of file */
