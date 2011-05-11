/* C stubs for OCaml OCI wrapper - connecting and disconnecting */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <oci.h>
#include "oci_wrapper.h"

/* Create an OCI environment */
OCIEnv* global_env;

/* final clean disconnection from shared memory - OCITerminate actually needed only when connected locally */
void oci_final_cleanup(void) {
#ifdef DEBUG
  debug("oci_final_cleanup: entered");
#endif
 if (global_env) {
    OCIHandleFree((dvoid*)global_env, OCI_HTYPE_ENV);
  }
  
  OCITerminate(OCI_DEFAULT);
}

value caml_oci_env_create(value unit) {
#ifdef DEBUG
  debug("caml_oci_env_create: entered");
#endif

  CAMLparam1(unit);
  sword x = OCIEnvCreate(&global_env, OCI_DEFAULT, 0, 0, 0, 0, 0, 0);
  if (x != OCI_SUCCESS) {
    raise_caml_exception(-1, "Cannot create an OCI environment (check ORACLE_HOME?)");
  }

#ifdef DEBUG
  debug("caml_oci_env_create: new env created");
#endif

  atexit(oci_final_cleanup);
  value v = caml_alloc_custom(&oci_custom_ops, sizeof(OCIEnv*), 0, 1);
  Oci_env_val(v) = global_env;
  CAMLreturn(v);
}

/* allocate error, server, service context and session handles within the environment and package them nicely */
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
    oci_non_success(h);
  }
  /* place the attached server within the service context */
  OCIAttrSet((dvoid*)h.svc, OCI_HTYPE_SVCCTX, (dvoid*)h.srv, 0, OCI_ATTR_SERVER, h.err);
  CAMLreturn(Val_unit);
}

/* set an attribute within a session */
value caml_oci_sess_set_attr(value handles, value attr_name, value attr_value) {
  CAMLparam3(handles, attr_name, attr_value);
  oci_handles_t h = Oci_handles_val(handles);
  char* v = String_val(attr_value);
  int n = Int_val(attr_name);
  OCIAttrSet(h.ses, OCI_HTYPE_SESSION,(dvoid *)v, strlen(v), n, h.err);
  
  CAMLreturn(Val_unit);
}

/* Actually start a session. It is probably quite inefficient to keep coercing 
   my handles across the C/OCaml boundary but other than making them global
   and forfeiting having multiple sessions open I'm not sure what to do */
value caml_oci_session_begin(value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);
  OCIAttrSet ((void*)h.svc, OCI_HTYPE_SVCCTX, (void*)h.srv, 0, OCI_ATTR_SERVER,h.err);
  sword x = OCISessionBegin ((void*)h.svc, h.err, h.ses, OCI_CRED_RDBMS, OCI_DEFAULT);
  if (x != OCI_SUCCESS)  {
    oci_non_success(h);
  }
  /* place the session within the service context */
  OCIAttrSet ((void*)h.svc, OCI_HTYPE_SVCCTX, (void*)h.ses, 0, OCI_ATTR_SESSION, h.err);
  CAMLreturn(Val_unit);
}

/* do a pointless query to force v$session.module to update */
value caml_oci_set_module(value env, value handles, value module) {
  CAMLparam3(env, handles, module);
  OCIEnv* e = Oci_env_val(env);
  oci_handles_t h = Oci_handles_val(handles);
  char* m = String_val(module);

  OCIAttrSet(h.ses, OCI_HTYPE_SESSION,(void *)m, strlen(m), OCI_ATTR_MODULE, h.err);
  
  run_sql_simple(e, h, "begin null; end;");

  CAMLreturn(Val_unit);
}

/* end the current session (so we will be logged off but still connected) */
value caml_oci_session_end (value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);

  sword x = OCISessionEnd(h.svc, h.err, h.ses, 0);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }

  CAMLreturn(Val_unit);
}

/* detach from the server */
value caml_oci_server_detach(value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);

  sword x = OCIServerDetach(h.srv, h.err, OCI_DEFAULT);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }

  CAMLreturn(Val_unit);
}

/* de-allocate all the handles */
value caml_oci_free_handles(value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);

  /* first the handles, if allocated */
  if (h.srv) {
    OCIHandleFree((dvoid*)h.srv, OCI_HTYPE_SERVER);
  }

  if (h.svc) {
    OCIHandleFree((dvoid*)h.svc, OCI_HTYPE_SVCCTX);
  }

  if (h.err) {
    OCIHandleFree((dvoid*)h.err, OCI_HTYPE_ERROR);
  }

  if (h.ses) {
    OCIHandleFree((dvoid*)h.ses, OCI_HTYPE_SESSION);
  }

  CAMLreturn(Val_unit);
}

value caml_oci_terminate(value unit) {
  CAMLparam1(unit);

  /* then the environment itself - moved here because we may wish to close a connection but retain the environment */
  oci_final_cleanup();
  CAMLreturn(Val_unit);
}

/* end of file */
